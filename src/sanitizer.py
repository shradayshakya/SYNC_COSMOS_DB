from faker import Faker

faker = Faker()

# Mapping of field names to faker generators (case-insensitive match)
SANITIZE_FIELDS = {
    "firstname": lambda: faker.first_name(),
    "lastname": lambda: faker.last_name(),
    "fullname": lambda: f"{faker.first_name()} {faker.last_name()}",
    "name": lambda: f"{faker.first_name()} {faker.last_name()}",
    "ssn": lambda: faker.ssn(),
    "phonenumber": lambda: faker.phone_number(),
    "mobilenumber": lambda: faker.phone_number(),
    "email": lambda: faker.email(),
    "workemail": lambda: faker.company_email(),
    "personalemail": lambda: faker.free_email(),
    "address": lambda: faker.address(),
    "street": lambda: faker.street_address(),
    "city": lambda: faker.city(),
    "state": lambda: faker.state(),
    "postalcode": lambda: faker.postcode(),
    "zip": lambda: faker.postcode(),
    "jobtitle": lambda: faker.job(),
    "department": lambda: faker.bs(),
    "dateofbirth": lambda: faker.date_of_birth().isoformat(),
    "managerid": lambda: faker.uuid4(),
    "insurance": lambda: faker.bothify(text="INS-####-####"),
    "taxid": lambda: faker.ssn(),
    "accountname": lambda: faker.company(),
    "accountnumber": lambda: faker.bban(),
    "routingnumber": lambda: faker.random_number(digits=9, fix_len=True),
    "line1": lambda: faker.street_address(),
    "line2": lambda: faker.secondary_address(),
    "countyname": lambda: faker.city(),
    "countyfips": lambda: faker.random_number(digits=5, fix_len=True),
    "ratingarea": lambda: faker.random_int(min=1, max=5),
    "payrate": lambda: round(faker.pyfloat(min_value=15.0, max_value=150.0), 2)
}


def sanitize_document_recursive(doc):
    """
    Recursively sanitize a document by replacing PII fields with fake values.
    This function modifies the document in-place and returns it.
    """
    if isinstance(doc, dict):
        for key in doc:
            value = doc[key]
            lowered_key = key.lower()

            if lowered_key in SANITIZE_FIELDS:
                try:
                    doc[key] = SANITIZE_FIELDS[lowered_key]()
                except Exception:
                    doc[key] = "REDACTED"
            elif isinstance(value, (dict, list)):
                sanitize_document_recursive(value)

    elif isinstance(doc, list):
        for item in doc:
            sanitize_document_recursive(item)

    return doc
