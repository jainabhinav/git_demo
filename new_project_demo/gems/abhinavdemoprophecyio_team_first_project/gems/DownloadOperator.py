# put your spec here
from pyspark.sql import *

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext


class FileOperation(ComponentSpec):
    name: str = "DownloadOperation"
    category: str = "Custom"
    gemDescription: str = "Helps perform file operations like copy and move on different file systems"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/custom/file-operations"


    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class FileOperationProperties(ComponentProperties):
        Operation: str = "Download"
        source: str = ""
        destination: str = ""
        sharepoint_url: str = ""
        file_name: str = ""

        filesystem: str = "Sharepoint"

    def dialog(self) -> Dialog:
        fsSelectBox = (SelectBox("Operation")
                       .addOption("Download", "Download")
                       .addOption("Upload", "Upload")
                       .bindProperty("Operation"))
        return Dialog("PathSelection") \
        .addElement(ColumnsLayout(gap="1rem", height="100%")
                    .addColumn(StackLayout(height="100%")
                    .addElement(fsSelectBox)

            
                                  # Binds the selected action to "sharepoint_action"                         
                                .addElement(TextBox("Sharepoint Url", placeholder="Eg: /dbfs/Prophecy/test.csv")
                                           .bindProperty("sharepoint_url"))
                               .addElement(TextBox("Source Path", placeholder="Eg: /dbfs/Prophecy/test.csv")
                                           .bindProperty("source"))
                                .addElement(TextBox("File Name", placeholder="For Download all files keep it empty or give *")
                                           .bindProperty("file_name"))
                               .addElement(TextBox("Destination Path", placeholder="Eg: /dbfs/Prophecy/destination/test.csv")
                                           .bindProperty("destination")),
                               "2fr")
                    )

    def validate(self, context: WorkflowContext, component: Component[FileOperationProperties]) -> List[Diagnostic]:
        diagnostics = []
        if isBlank(component.properties.sharepoint_url):
            diagnostics.append(
                Diagnostic("properties.sharepoint_url", "sharepoint_url has to be specified",
                           SeverityLevelEnum.Error))
        if isBlank(component.properties.source):
            diagnostics.append(
                Diagnostic("properties.source", "Source path has to be specified",
                           SeverityLevelEnum.Error))
        if isBlank(component.properties.destination):
            diagnostics.append(
                Diagnostic("properties.destination", "Destination path has to be specified",
                           SeverityLevelEnum.Error))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[FileOperationProperties], newState: Component[FileOperationProperties]) -> \
            Component[FileOperationProperties]:
        newState = replace(newState, ports=replace(newState.ports, isCustomOutputSchema=True))
        return newState

    class FileOperationCode(ComponentCode):
        def __init__(self, newProps):
            self.props: FileOperation.FileOperationProperties = newProps

        def apply(self, spark: SparkSession):
            source = self.props.source
            dest = self.props.destination
            sharepoint_url = self.props.sharepoint_url
            file_name = self.props.file_name
            Operation=self.props.Operation

            import os
            from office365.sharepoint.client_context import ClientContext
            from office365.runtime.auth.user_credential import UserCredential

            env: SubstituteDisabled = os.environ['ENVIRONMENT']
            
            sharepoint_userid: SubstituteDisabled = dbutils.secrets.get(scope="dta-eun-kv-dbk-01", key="sharepoint-userid")
            sharepoint_password: SubstituteDisabled = dbutils.secrets.get(scope="dta-eun-kv-dbk-01", key="sharepoint-password")

            ctx: SubstituteDisabled = ClientContext(sharepoint_url).with_credentials(UserCredential(f"{sharepoint_userid}", f"{sharepoint_password}"))

            #dest = f"/Volumes/beam_ndev/uc_volumes/enggmetricsinbound/sharepoint3"
            #file_name='BAU Changes Tracker.xlsx'
            # Ensure the directory exists
            if Operation == 'Download':
                os.makedirs(dest, exist_ok=True)

                if len(file_name) == 0 or file_name == "*":
                    # Get the folder from SharePoint
                    folder: SubstituteDisabled = ctx.web.get_folder_by_server_relative_url(source)

                    # Load the files in the folder
                    files: SubstituteDisabled = folder.files
                    ctx.load(files)
                    ctx.execute_query()

                    print("Displaying Files: ")

                    # Iterate through each file in the folder
                    for file in files:
                        file_name = file.properties["Name"]
                        print(file_name)

                        # Full local file path where the file will be saved
                        local_file_path = os.path.join(dest, file_name)

                        # Download the file to the specified local path
                        try:
                            with open(local_file_path, "wb") as local_file:
                                file.download(local_file)
                                ctx.execute_query()  # Execute the download query
                            print(f"File '{file_name}' downloaded and saved to: {local_file_path}")
                        except Exception as e:
                            print(f"Error downloading file '{file_name}': {str(e)}")

                    print("All files have been downloaded.")

                else:
                    # Download specific file
                    sharepoint_file_url = f"{source}/{file_name}"
                    try:
                        file: SubstituteDisabled = ctx.web.get_file_by_server_relative_url(sharepoint_file_url)
                        ctx.load(file)
                        ctx.execute_query()

                        local_file_path = os.path.join(dest, file_name)
                        with open(local_file_path, "wb") as local_file:
                            file.download(local_file)
                            ctx.execute_query()
                        print(f"File '{file_name}' downloaded and saved to: {local_file_path}")

                    except Exception as e:
                        print(f"Error downloading the file '{file_name}': {str(e)}")

            # If uploading files
            else:
                try:
                    # Try to get the folder by its server relative URL
                    folder: SubstituteDisabled = ctx.web.get_folder_by_server_relative_url(dest)
                    ctx.load(folder)
                    ctx.execute_query()  # Check if the folder exists
                    
                    print(f"Folder '{dest}' already exists.")

                except Exception as e:
                    if 'FileNotFound' in str(e):  # Folder doesn't exist
                        print(str(e))
                        print(f"Folder '{dest}' does not exist. Creating new folder...")
                        
                        # Extract the parent folder URL and folder name from the destination path
                        parent_folder_url: SubstituteDisabled = '/'.join(dest.split('/')[:-1])  # Get parent folder URL
                        folder_name = dest.split('/')[-1]  # Get the folder name
                        
                        # Create the folder by adding it under the parent folder
                        parent_folder: SubstituteDisabled = ctx.web.get_folder_by_server_relative_url(parent_folder_url)
                        created_folder: SubstituteDisabled = parent_folder.folders.add(folder_name)
                        ctx.execute_query()  # Execute the creation request
                        
                        print(f"Folder '{dest}' created successfully.")
                    else:
                        print(f"Error occurred while checking folder: {str(e)}")

                print("Destination Directory Validation Done")
                
                # Uploading files logic (if file_name is provided or uploading all files)
                files: SubstituteDisabled = dbutils.fs.ls(source)
                print(file_name)
                print(len(file_name))
                if len(file_name)==0 or file_name == "*":
                    for file in files:
                        if not file.isDir():
                            print("All files are uploading from source")
                            print(f"Uploading file: {file.path}")
                            try:
                                with open(file.path.replace('dbfs:', ''), "rb") as f:
                                    folder.upload_file(os.path.basename(file.path), f.read())  # Upload each file
                                    ctx.execute_query()
                                    print(f"File '{os.path.basename(file.path)}' uploaded successfully to SharePoint at: {dest}")
                            except Exception as upload_error:
                                print(f"Error uploading file '{os.path.basename(file.path)}': {str(upload_error)}")
                    

                else:
                    # Upload all files if no specific file name is provided
                    for file in files:
                        print("Uploading Single File")
                        if not file.isDir() and os.path.basename(file.path) == file_name:
                            print(f"Uploading file: {file.path}")
                            try:
                                with open(file.path.replace('dbfs:', ''), "rb") as f:
                                    folder.upload_file(file_name, f.read())  # Upload the file
                                    ctx.execute_query()
                                    print(f"File '{file_name}' uploaded successfully to SharePoint at: {dest}")
                            except Exception as upload_error:
                                print(f"Error uploading file '{file_name}': {str(upload_error)}")
                            break  # Stop after uploading the specified file

                    



