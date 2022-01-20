import subprocess

pdf_file_path = 'sep_21.pdf'
outputFilename = "a.html"
# with open(pdf_file_path, 'rb') as book:
subprocess.run(["pdf2htmlEX", pdf_file_path, outputFilename])
