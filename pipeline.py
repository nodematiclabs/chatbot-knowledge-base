import kfp
import kfp.dsl as dsl

from kfp import compiler
from kfp.dsl import Dataset, Input, Output

from typing import Dict, List

@dsl.component(
    base_image='python:3.11',
    packages_to_install=['appengine-python-standard']
)
def get_matching_files(directory: str, pattern: str) -> List[str]:
    import os
    import re

    # Compile the regex pattern
    regex = re.compile(pattern)
    
    # List to store matching file paths
    matching_files = []

    # Walk through directory including subdirectories
    for root, dirs, files in os.walk(directory.replace("gs://", "/gcs/")):
        for file in files:
            # If file name matches the pattern, add it to the list
            if regex.match(os.path.join(root, file).replace(directory.replace("gs://", "/gcs/"), "")):
                # os.path.join concatenates root, dirs, and file into a full path
                matching_files.append(os.path.join(root, file))

    # Return the list of matching files
    return [matching_file.replace("/gcs/", "gs://") for matching_file in matching_files]

@dsl.component(
    base_image='python:3.11',
    packages_to_install=['pypdf2==2.12.1', 'appengine-python-standard']
)
def split_pdf_into_pages(pdf_file: str) -> List[str]:
    import os
    import PyPDF2

    page_files = []

    # Open the PDF file
    with open(pdf_file.replace("gs://", "/gcs/"), 'rb') as file:
        # Create a PDF reader object
        pdf_reader = PyPDF2.PdfFileReader(file)

        # Get the total number of pages in the PDF
        total_pages = pdf_reader.numPages

        # Iterate through each page and save it as a separate PDF
        for page_number in range(total_pages):
            # Get a page
            pdf_page = pdf_reader.getPage(page_number)

            # Create a PDF writer object
            pdf_writer = PyPDF2.PdfFileWriter()

            # Add the page to the writer
            pdf_writer.addPage(pdf_page)

            # Output file name
            output_file_path = pdf_file.replace("gs://", "/gcs/").replace(".pdf", "/") + \
                pdf_file.split("/")[-1].replace(".pdf", f".{page_number + 1}.pdf")

            # Create the directory if it doesn't exist
            os.makedirs(
                os.path.dirname(pdf_file.replace("gs://", "/gcs/").replace(".pdf", "/")),
                exist_ok=True
            )

            # Save the page as a PDF file
            with open(output_file_path, 'wb') as output_file:
                pdf_writer.write(output_file)
            page_files.append(output_file_path.replace("/gcs/", "gs://"))

    print(f'Successfully split the PDF into {total_pages} pages')
    return page_files

@dsl.component(
    base_image='python:3.11',
    packages_to_install=['google-cloud-documentai', 'appengine-python-standard']
)
def parse_text(pdf_file: str) -> str:
    from google.cloud import documentai
    from google.api_core.client_options import ClientOptions

    project_id = 'TODO'
    location = 'us'
    mime_type = 'application/pdf'
    processor_id = 'TODO'

    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=opts)
    name = client.processor_path(project_id, location, processor_id)
    with open(pdf_file.replace("gs://", "/gcs/"), "rb") as image:
        image_content = image.read()
    raw_document = documentai.RawDocument(content=image_content, mime_type=mime_type)
    request = documentai.ProcessRequest(name=name, raw_document=raw_document)
    result = client.process_document(request=request)
    document = result.document

    with open(pdf_file.replace("gs://", "/gcs/").replace(".pdf", ".txt"), 'w') as file:
        file.write(document.text)
    return pdf_file.replace(".pdf", ".txt")


@dsl.component(
    base_image='python:3.11',
    packages_to_install=['google-cloud-aiplatform', 'appengine-python-standard']
)
def generate_embedding(txt_file: str) -> Dict:
    from vertexai.language_models import TextEmbeddingModel

    model = TextEmbeddingModel.from_pretrained("textembedding-gecko@001")

    with open(txt_file.replace("gs://", "/gcs/"), 'r') as f:
        text = f.read()
        embeddings = model.get_embeddings([text])
        embedding = embeddings[0].values

    return {"id": txt_file, "embedding": embedding}

@dsl.component(
    base_image='python:3.11',
    packages_to_install=['elasticsearch', 'appengine-python-standard']
)
def write_embeddings(embedding: Dict):
    from elasticsearch import Elasticsearch

    # Connect to the Elasticsearch instance
    es = Elasticsearch(
        hosts=["http://TODO:9200"],
        basic_auth=("elastic", "TODO")
    )

    # Name of the index
    index_name = "technology_papers_and_reports"

    # Define the mapping for the index
    mapping = {
        "mappings": {
            "properties": {
                "embedding": {
                    "type": "dense_vector",
                    "dims": 768
                }
            }
        }
    }

    # Create the index with the mapping
    es.indices.create(index=index_name, body=mapping, ignore=400)

    # Index the vector embeddings
    es.index(index=index_name, id=embedding["id"], body={"embedding": embedding["embedding"]})

    print("Embeddings indexed successfully.")


@dsl.pipeline(
    name="technology-papers-and-reports",
)
def technology_papers_and_reports(gcs_directory: str):
    get_matching_files_task = get_matching_files(
        directory=gcs_directory,
        pattern="^[^/]*\.pdf"
    )
    with dsl.ParallelFor(
        name="pdf-parsing",
        items=get_matching_files_task.output,
        parallelism=3
    ) as pdf_file:
        split_pdf_into_pages_task = split_pdf_into_pages(
            pdf_file=pdf_file
        )
        with dsl.ParallelFor(
            name="pdf-page-parsing",
            items=split_pdf_into_pages_task.output,
            parallelism=3
        ) as pdf_page_file:
            parse_text_task = parse_text(
                pdf_file=pdf_page_file
            )
            generate_embedding_task = generate_embedding(
                txt_file=parse_text_task.output
            )
            write_embeddings_task = write_embeddings(
                embedding=generate_embedding_task.output
            )


compiler.Compiler().compile(technology_papers_and_reports, 'pipeline.json')