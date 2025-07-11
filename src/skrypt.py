import os

def save_files_content_to_txt(root_folder, output_file, skip_dirs=None, extensions=None):
    if skip_dirs is None:
        skip_dirs = {'__pycache__'}
    if extensions is None:
        extensions = {'.py', '.md'}

    with open(output_file, 'w', encoding='utf-8') as outfile:
        for root, dirs, files in os.walk(root_folder):
            # Pomijaj wybrane katalogi
            dirs[:] = [d for d in dirs if d not in skip_dirs]

            for file in files:
                _, ext = os.path.splitext(file)
                if ext in extensions:
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as infile:
                            outfile.write(f"==== {file_path} ====" + os.linesep)
                            outfile.write(infile.read() + os.linesep * 2)
                    except Exception as e:
                        print(f"Nie udało się odczytać pliku {file_path}: {e}")

if __name__ == "__main__":
    # Zmień 'input_folder' na folder, który chcesz przeszukać
    input_folder = "./"
    output_file = "output.txt"
    # Możesz tu dodać inne katalogi do pominięcia, np. '.git'
    skip_dirs = {'__pycache__', '.git'}
    # Możesz tu dodać inne rozszerzenia plików
    extensions = {'.py', '.json'}

    save_files_content_to_txt(input_folder, output_file, skip_dirs, extensions)
    print(f"Zawartość plików została zapisana do {output_file}")
