import os

def process_directory(directory):
    # Loop through all files and subdirectories in the current directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.gzip'):
                gzip_file_path = os.path.join(root, file)
                gz_file_path = gzip_file_path[:-5] + ".gz"  # Correct the extension

                # Check if a file with the .gz extension exists
                if os.path.exists(gz_file_path):
                    # If exists, delete the .gz file
                    os.remove(gzip_file_path)
                    # print(f"Deleted {gzip_file_path}")
                else:
                    # Rename the .gzip file to .gz
                    os.rename(gzip_file_path, gz_file_path)
                    print(f"Renamed {gzip_file_path} to {gz_file_path}")

# Main function
def main():
    directory = input("Enter the directory path (Press Enter to use the current directory): ")
    if directory == "":
        directory = os.getcwd()  # Use current directory if no path is provided
    process_directory(directory)

if __name__ == "__main__":
    main()
