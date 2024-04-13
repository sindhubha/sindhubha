==========================
list - campus_is,mill_date
==========================

today - 11-12-2023

res = select
    ifnull(msed.id,'')
    mse.id,
    mse.esn,
    0 as pd,
    ifnull(td,0), -- end_kwh
    ifnull(tc,0), -- consumption
from
    mse
    left join msed
        on
            mse.campus_id=msed.campus_id
            and mse.id=msed.energy_src_id
            and msed.mill_date=''
            and mse.per_ty='date'

previouse  - (11-12-2023 - 1)
res_p = select
    mse.id,
    mse.esn,
    0 as pd,
    ifnull(td,0), -- end_kwh
    ifnull(tc,0), -- consumption
from
    mse
    left join msed
        on
            mse.campus_id=msed.campus_id
            and mse.id=msed.energy_src_id
            and msed.mill_date=''
            and mse.per_ty='date'

for(res as row){
    for(res_p as row_p){
        if(row->id==row_p->id)
        {
            row->pd=row_p->td
        }
    }
}

return res_today,res_month
==========================


==========================
save_src_entry_data

[
    {
        id:'',
        src_id:'',
        end_kwh:'',
        consumption:'',
        campus_id:'',
        mill_date:''
    },{
        id:'',
        src_id:'',
        end_kwh:'',
        consumption:'',
        campus_id:'',
        mill_date:''
    },{
        id:'',
        src_id:'',
        end_kwh:'',
        consumption:'',
        campus_id:'',
        mill_date:''
    }
]