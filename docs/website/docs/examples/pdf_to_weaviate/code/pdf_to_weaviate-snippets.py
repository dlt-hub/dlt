from tests.pipeline.utils import assert_load_info

def pdf_to_weaviate_snippet() -> None:
    # @@@DLT_SNIPPET_START example
    # @@@DLT_SNIPPET_START pdf_to_weaviate
    import os

    import dlt
    from dlt.destinations.impl.weaviate import weaviate_adapter
    from PyPDF2 import PdfReader


    @dlt.resource(selected=False)
    def list_files(folder_path: str):
        folder_path = os.path.abspath(folder_path)
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            yield {
                "file_name": filename,
                "file_path": file_path,
                "mtime": os.path.getmtime(file_path)
            }


    @dlt.transformer(primary_key="page_id", write_disposition="merge")
    def pdf_to_text(file_item, separate_pages: bool = False):
        if not separate_pages:
            raise NotImplementedError()
        # extract data from PDF page by page
        reader = PdfReader(file_item["file_path"])
        for page_no in range(len(reader.pages)):
            # add page content to file item
            page_item = dict(file_item)
            page_item["text"] = reader.pages[page_no].extract_text()
            page_item["page_id"] = file_item["file_name"] + "_" + str(page_no)
            yield page_item

    pipeline = dlt.pipeline(
        pipeline_name='pdf_to_text',
        destination='weaviate'
    )

    # this constructs a simple pipeline that: (1) reads files from "invoices" folder (2) filters only those ending with ".pdf"
    # (3) sends them to pdf_to_text transformer with pipe (|) operator
    pdf_pipeline = list_files("assets/invoices").add_filter(
        lambda item: item["file_name"].endswith(".pdf")
    ) | pdf_to_text(separate_pages=True)

    # set the name of the destination table to receive pages
    # NOTE: Weaviate, dlt's tables are mapped to classes
    pdf_pipeline.table_name = "InvoiceText"

    # use weaviate_adapter to tell destination to vectorize "text" column
    load_info = pipeline.run(
        weaviate_adapter(pdf_pipeline, vectorize="text")
    )
    row_counts = pipeline.last_trace.last_normalize_info
    print(row_counts)
    print("------")
    print(load_info)
    # @@@DLT_SNIPPET_END pdf_to_weaviate

    # @@@DLT_SNIPPET_START pdf_to_weaviate_read
    import weaviate

    client = weaviate.Client("http://localhost:8080")
    # get text of all the invoices in InvoiceText class we just created above
    print(client.query.get("InvoiceText", ["text", "file_name", "mtime", "page_id"]).do())
    # @@@DLT_SNIPPET_END pdf_to_weaviate_read
    # @@@DLT_SNIPPET_END example
    assert_load_info(load_info)
