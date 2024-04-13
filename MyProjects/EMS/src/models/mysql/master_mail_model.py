from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response

async def mail_list(cnx,mail_id):
    where =''
    if mail_id != '':
        where += f" and  mail_id = '{mail_id}'"
    query = text(f'''select * from ems_v1.master_mail where status != 'delete' {where}''')
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
    

async def save_mail(cnx,from_mail,to_mail,pwd,cc_mail,bcc_mail,subject,compose_textarea,report,mail_type,send_at,send_day,user_login_id,campus_id):
   
    query = text(f'''insert into ems_v1.master_mail (from_mail,to_mail,pwd,cc_mail,bcc_mail,subject,compose_textarea,report,mail_type,send_at,send_day,created_on , created_by,campus_id)
                     values('{from_mail}','{to_mail}','{pwd}','{cc_mail}','{bcc_mail}','{subject}','{compose_textarea}','{report}','{mail_type}','{send_at}','{send_day}', now(), {user_login_id},'{campus_id}')''')

    await cnx.execute(query)
    await cnx.commit()
    
async def update_mail(cnx,mail_id,from_mail,to_mail,pwd,cc_mail,bcc_mail,subject,compose_textarea,report,mail_type,send_at,send_day,user_login_id,campus_id):
    query = text(f'''update ems_v1.master_mail 
                        set from_mail = '{from_mail}', to_mail = '{to_mail}', pwd = '{pwd}',
                        cc_mail = '{cc_mail}', bcc_mail = '{bcc_mail}', subject = '{subject}', 
                        compose_textarea = '{compose_textarea}', report = '{report}', 
                        mail_type = '{mail_type}', send_at = '{send_at}', send_day = '{send_day}', modified_on = now(),campus_id = '{campus_id}',
                        modified_by = {user_login_id} where mail_id = {mail_id} ''')
    await cnx.execute(query)
    await cnx.commit()

async def update_mailstatus(cnx, mail_id, status):

    if status != '':
        query = text(f"update master_mail set status = '{status}'  where mail_id = '{mail_id}' ")
    else:
        query = text(f"update master_mail set status = 'delete'  where mail_id = '{mail_id}' ")
    await cnx.execute(query)
    await cnx.commit()