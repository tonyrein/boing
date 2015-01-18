"""
    Interface with classes used to communicate with local storage (sqlite database)
"""

class ServiceLocal(object):
    def __init__(self, dao_object):
        if dao_object is None:
            raise ValueError("ServiceLocal class needs a dao local object")
        self._do = dao_object
        
    def get_non_processed(self):
        return self._do.list_where("es_id = ''")
    
    def update_with_es_id(self, db_id, es_id):
        where_clause = "db_id = " + str(db_id)
        fields = ( 'es_id', )
        new_values = ( es_id, )
        self._do.update_where(fields, new_values, where_clause)
    
    def delete_finished_records(self):
        return self._do.delete_where("es_id != ''")
    
    def write_new_records(self, records):
        return self._do.insert_bulk(records)
        
    def write_single_record(self, record):
        self._do.insert_single(record)
        
        