from elasticsearch_dsl import DocType
from elasticsearch_dsl.field import String, Date


class Call(DocType):
    lat = String()
    lng = String()
    desc = String()
    zip = String()
    title = String()
    timestamp = Date()
    twp = String()
    addr = String()

    class Meta:
        index = 'calls'
