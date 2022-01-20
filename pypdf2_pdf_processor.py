import PyPDF2 as pdf

pdf_file_path = r'G:\My Drive\bills\sep 21.pdf'
with open(pdf_file_path, 'rb') as book:
    pdf_reader = pdf.PdfFileReader(book)
    # pages = pdf_reader.numPages
    page = pdf_reader.getPage(2609)
    text = page.extractText()

with open('a.txt', 'w') as writer:
    writer.write(text)

print(text)
