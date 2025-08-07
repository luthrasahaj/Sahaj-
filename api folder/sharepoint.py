from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.files.file import File
import os

def download_all_files_from_folder():
    site_url = os.getenv("SHAREPOINT_SITE")
    username = os.getenv("SP_USERNAME")
    password = os.getenv("SP_PASSWORD")
    folder_url = os.getenv("SP_FOLDER_URL")

    ctx_auth = AuthenticationContext(site_url)
    if not ctx_auth.acquire_token_for_user(username, password):
        raise Exception("❌ SharePoint Auth failed")

    ctx = ClientContext(site_url, ctx_auth)
    folder = ctx.web.get_folder_by_server_relative_url(folder_url)
    files = folder.files
    ctx.load(files)
    ctx.execute_query()

    os.makedirs("downloads", exist_ok=True)
    downloaded_files = []

    for file in files:
        name = file.properties["Name"]
        local_path = f"downloads/{name}"

        if os.path.exists(local_path):
            continue  # ✅ skip already downloaded

        if name.endswith(".csv") or name.endswith(".xlsx"):
            file_url = folder_url + "/" + name
            response = File.open_binary(ctx, file_url)
            with open(local_path, "wb") as f:
                f.write(response.content)
            downloaded_files.append(local_path)

    return downloaded_files
