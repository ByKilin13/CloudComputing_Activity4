from tortoise import fields
from tortoise.models import Model

class UserDB(Model):
    id = fields.IntField(pk=True)
    username = fields.CharField(min_length=3, max_length=50)
    password = fields.CharField(min_length=3, max_length=100)
    mail = fields.CharField(min_length=3, max_length=50)
    year_of_birth = fields.IntField(min_length=3, max_length=50, null=True)
