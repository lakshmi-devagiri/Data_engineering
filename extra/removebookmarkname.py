import fitz  # PyMuPDF


def remove_text_watermark(input_pdf, output_pdf, text_to_remove):
    # Open the input PDF
    pdf = fitz.open(input_pdf)

    # Iterate over each page
    for page_number in range(len(pdf)):
        page = pdf[page_number]
        text_instances = page.search_for(text_to_remove)

        # If watermark text is found, redact (hide) it
        for inst in text_instances:
            page.add_redact_annot(inst, fill=(255, 255, 255))  # White color
        page.apply_redactions()  # Apply the redactions permanently

    # Save the updated PDF
    pdf.save(output_pdf)

remove_text_watermark(r"D:\bigdata\drivers\docs\pyspark-problem-statements-and-answers.pdf", r"D:\bigdata\drivers\docs\pyspark-problem-statements-answers.pdf","WWW.LINKEDIN.COM/IN/AKASHMAHINDRAKAR")