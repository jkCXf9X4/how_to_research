

class FileUtils:

    def doi_filename(doi, ext="json"):
        return f"{doi.replace("/", "_")}.{ext}"