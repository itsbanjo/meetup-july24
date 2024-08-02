import random
import csv
import string
import requests
import argparse
import os
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, Image
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics

# Register DejaVuSans font
pdfmetrics.registerFont(TTFont('DejaVuSans', 'artefacts/DejaVuSans.ttf'))

class BloodParameter:
    def __init__(self, name, unit, lower_bound, upper_bound):
        self.name = name
        self.unit = unit
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound

    def generate_random_value(self, percentage_min, percentage_max):
        range_extension = (self.upper_bound - self.lower_bound) * (random.uniform(percentage_min, percentage_max) / 100)
        extended_lower_bound = self.lower_bound - range_extension
        extended_upper_bound = self.upper_bound + range_extension
        return random.uniform(extended_lower_bound, extended_upper_bound)

    def is_within_range(self, value):
        return self.lower_bound <= value <= self.upper_bound

def generate_report(parameters, measurements, silent=False):
    report = ""
    for param in parameters:
        value = measurements[param.name]
        ref_range = f"({param.lower_bound} - {param.upper_bound})"
        if not silent:
            within_range = param.is_within_range(value)
            status = "within range" if within_range else "out of range"
            report += f"{param.name} ({param.unit}): {value:.2f} {ref_range} - {status}\n"
        else:
            report += f"{param.name} ({param.unit}): {value:.2f} {ref_range}\n"
    return report

def generate_samples(parameters, num_samples, percentage_min, percentage_max):
    samples = []
    for _ in range(num_samples):
        measurements = {param.name: param.generate_random_value(percentage_min, percentage_max) for param in parameters}
        samples.append(measurements)
    return samples

def get_random_user():
    response = requests.get('https://randomuser.me/api/?nat=nz')
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        full_name = f"{user_data['name']['first']} {user_data['name']['last']}"
        address = f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}, {user_data['location']['city']}, {user_data['location']['postcode']}, {user_data['location']['state']}, New Zealand"
        sex = user_data['gender']
        age = user_data['dob']['age']
        dob = user_data['dob']['date'].split('T')[0]
        return full_name, address, sex, age, dob
    else:
        return "John Doe", "123 Fake Street, Faketown, 0000, FakeState, New Zealand", "male", 30, "1981-01-01"

def generate_nhi():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=7))

def get_lab_location():
    nz_cities = ["Auckland", "Wellington", "Christchurch", "Hamilton", "Tauranga", "Napier-Hastings", "Dunedin", "Palmerston North", "Nelson", "Rotorua"]
    return random.choice(nz_cities)

def generate_lab_number():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

def save_as_pdf(patient, samples, parameters, output_dir):
    full_name, address, nhi, sex, age, dob, lab = patient
    for i, (year, measurements, collection_date, lab_number) in enumerate(samples):
        report = generate_report(parameters, measurements)
        file_name = f"{output_dir}/Report_{full_name.replace(' ', '_')}_{i+1}_{year}.pdf"
        doc = SimpleDocTemplate(file_name, pagesize=letter)
        elements = []

        # Add Lab logo and address on the same row
        table_data = [[
            Image("artefacts/logo.jpg", width=50, height=50),
            Paragraph("Random Lab<br/>PO Box 12345, Faketown, New Zealand", getSampleStyleSheet()["Normal"])
        ]]
        logo_address_table = Table(table_data, colWidths=[60, 400])
        elements.append(logo_address_table)
        elements.append(Spacer(1, 12))

        # Patient info table
        patient_data = [
            ["Patient:", full_name, "NHI:", nhi],
            ["Address:", address, "Sex:", sex],
            ["Age:", f"{age} years", "Date of birth:", dob],
            ["Lab:", lab, "", ""]
        ]
        table = Table(patient_data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        elements.append(table)
        elements.append(Spacer(1, 12))

        # Blood count heading
        elements.append(Paragraph("<b>BLOOD COUNT</b>", getSampleStyleSheet()["Title"]))
        elements.append(Spacer(1, 12))

        # Collection Date and Lab Number
        elements.append(Paragraph(f"Date: {collection_date}", getSampleStyleSheet()["Normal"]))
        elements.append(Paragraph(f"Lab Numbers: {lab_number}", getSampleStyleSheet()["Normal"]))
        elements.append(Spacer(1, 12))

        # Blood parameters table
        data = [["Parameter", "Measurement", "Ref. Range"]]
        for param in parameters:
            data.append([param.name, f"{measurements[param.name]:.2f} {param.unit}", f"{param.lower_bound} - {param.upper_bound}"])
        blood_table = Table(data)
        blood_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        elements.append(blood_table)

        # Build PDF
        doc.build(elements)
        print(f"Report saved as {file_name}")

def save_as_csv(patient, samples, parameters, output_dir):
    full_name, address, nhi, sex, age, dob, lab = patient
    for i, (year, measurements, collection_date, lab_number) in enumerate(samples):
        file_name = f"{output_dir}/Report_{full_name.replace(' ', '_')}_{i+1}_{year}.csv"
        with open(file_name, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Patient", full_name, "NHI", nhi])
            writer.writerow(["Address", address, "Sex", sex])
            writer.writerow(["Age", f"{age} years", "Date of birth", dob])
            writer.writerow(["Lab", lab])
            writer.writerow([])
            writer.writerow(["BLOOD COUNT"])
            writer.writerow(["Date", collection_date])
            writer.writerow(["Lab Numbers", lab_number])
            writer.writerow([])
            writer.writerow(["Parameter", "Measurement", "Ref. Range"])
            for param in parameters:
                writer.writerow([param.name, f"{measurements[param.name]:.2f} {param.unit}", f"{param.lower_bound} - {param.upper_bound}"])
        print(f"Report saved as {file_name}")

def main(patients, samples, start_year, end_year, percentage_min, percentage_max, silent, to_pdf, to_csv, output_dir):
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    parameters = [
        BloodParameter("Haemoglobin", "g/L", 130, 175),
        BloodParameter("RBC", "x10¹²/L", 4.30, 6.00),
        BloodParameter("HCT", "N/A", 0.40, 0.52),
        BloodParameter("MCV", "fL", 80, 99),
        BloodParameter("MCH", "pg", 27, 33.0),
        BloodParameter("Platelets", "x10⁹/L", 150, 400),
        BloodParameter("WBC", "x10⁹/L", 4.0, 11.0),
        BloodParameter("Neutrophils", "x10⁹/L", 1.90, 7.50),
        BloodParameter("Lymphocytes", "x10⁹/L", 1.00, 4.00),
        BloodParameter("Monoocytes", "x10⁹/L", 0.20, 1.00),
        BloodParameter("Eosinophils", "x10⁹/L", 0.00, 0.51),
        BloodParameter("Basophils", "x10⁹/L", 0.00, 0.20)
    ]

    for _ in range(patients):
        full_name, address, sex, age, dob = get_random_user()
        nhi = generate_nhi()
        lab = get_lab_location()
        patient = (full_name, address, nhi, sex, age, dob, lab)

        samples_data = []
        for year in range(start_year, end_year + 1):
            measurements = generate_samples(parameters, samples, percentage_min, percentage_max)
            for measurement in measurements:
                collection_date = datetime(year, random.randint(1, 12), random.randint(1, 28)).strftime('%d/%m/%Y')
                lab_number = generate_lab_number()
                samples_data.append((year, measurement, collection_date, lab_number))

        if to_pdf:
            save_as_pdf(patient, samples_data, parameters, output_dir)

        if to_csv:
            save_as_csv(patient, samples_data, parameters, output_dir)

        if not to_pdf and not to_csv:
            for year, measurements, collection_date, lab_number in samples_data:
                report = generate_report(parameters, measurements, silent)
                print(f"Patient: {full_name}\nNHI: {nhi}\nAddress: {address}\nSex: {sex}\nAge: {age} years\nDate of birth: {dob}\nLab: {lab}")
                print("\nBLOOD COUNT\n")
                print(f"Date: {collection_date}\nLab Numbers: {lab_number}\n")
                print(report)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate blood parameter reports.")
    parser.add_argument("--patients", type=int, required=True, help="Number of patients to generate")
    parser.add_argument("--samples", type=int, required=True, help="Number of samples to generate per patient")
    parser.add_argument("--start-year", type=int, required=True, help="Start year for the reports")
    parser.add_argument("--end-year", type=int, required=True, help="End year for the reports")
    parser.add_argument("--percentage-min", type=float, default=0, help="Minimum percentage to extend the range")
    parser.add_argument("--percentage-max", type=float, default=10, help="Maximum percentage to extend the range")
    parser.add_argument("--silent", action="store_true", help="Suppress the within range or out of range status in the report")
    parser.add_argument("--to-pdf", action="store_true", help="Save the reports as PDF files")
    parser.add_argument("--to-csv", action="store_true", help="Save the reports as CSV files")
    parser.add_argument("--output-dir", type=str, default=".", help="Directory to save the PDF/CSV files")

    args = parser.parse_args()

    main(args.patients, args.samples, args.start_year, args.end_year, args.percentage_min, args.percentage_max, args.silent, args.to_pdf, args.to_csv, args.output_dir)
