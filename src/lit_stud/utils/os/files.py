

class FileUtils:

    def doi_filename(doi, ext="json"):
        return f"{doi.replace("/", "_")}.{ext}"
    
    def from_doi_filename(filename:str):
        name = ".".join(filename.split(".")[:-1])
        return name.replace("_", "/")