import os
import shutil
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))



def delete_files_and_folders(folder_path):
    # Check if the folder exists
    if os.path.exists(folder_path):
        # Iterate through all files and subfolders
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            if os.path.isdir(item_path):
                # If it's a folder, delete it recursively
                shutil.rmtree(item_path)
            elif os.path.isfile(item_path) and item != "init.txt":
                # If it's a file and not "init.txt", delete it
                os.remove(item_path)
        print(f"All files and subfolders in {folder_path} have been deleted, except 'init.txt'.")
    else:
        print(f"The folder {folder_path} does not exist.")

# Example usage
folder_path = os.path.join(parent_path, 'annual_reports')
delete_files_and_folders(folder_path)