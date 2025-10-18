from faker import Faker
fake = Faker()

def test_faker_name():
    name = fake.name()
    assert isinstance(name, str) and len(name) > 0, "Name should be a non-empty string"
    