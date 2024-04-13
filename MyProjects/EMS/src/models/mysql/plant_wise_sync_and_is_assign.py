from sqlalchemy import text
from sqlalchemy import text


async def update_plant_wise_sync(cnx, table_name):
    query = f''' update plant_wise_sync Set sync_status = 'yes' Where table_name='{table_name}' '''
    await cnx.execute(text(query))
    await cnx.commit()

# def update_is_assign(cnx, table_name, table_name1, id, id1, column_name, column_name1):
    
#     query = f'''SELECT * FROM {table_name1} WHERE {column_name} = (SELECT {column_name} FROM {table_name1} WHERE {column_name1} = '{id}' ) AND status = 'active' '''
#     result = cnx.execute(text(query)).mappings().all()
    
#     if len(result) > 0:
#         update_query = f'''UPDATE {table_name} SET is_assign = 'yes' WHERE {column_name} = '{id1}' '''
#         cnx.execute(text(update_query))
#         cnx.commit()
#     else:
#         update_query = f'''UPDATE {table_name} SET is_assign = 'no' WHERE {column_name} = (SELECT {column_name} FROM {table_name1} WHERE {column_name1} = '{id}') '''
#         cnx.execute(text(update_query))
#         cnx.commit()