"""
Rule Name: ruefn_IDRP_Check_193
Domains: QS_V
QSTESTCD = PIJ0102 and QSORRES = Null. Query should fire
"""

def ruefn_IDRP_Check_193(query_text, QS_V):
    payload_records = []
    prim_df = QS_V.copy()
    if prim_df.shape[0] > 0:
        args_dict = {
			'p_form_ls': ['QS_PAMD2'],
			'p_visit_ls': [],
			'null_values': ['null', 'Null', 'NULL', 'NaN', '', ' ', 'None', 'NaT', 'np.nan', 'nan', None,np.nan],
			'logic_text': "((QSTESTCD.str.upper() == 'PIJ0102')and (QSORRES in @null_values))"
        }
        
        p_form_ls = args_dict['p_form_ls']
        p_visit_ls = args_dict['p_visit_ls']
		null_values = args_dict['null_values']
        logic_query = args_dict['logic_text']
        
        prim_df = udf_glbl_filter_by_formid_visitid(prim_df,p_form_ls,p_visit_ls)
        if prim_df.shape[0] > 0:
            prim_df = prim_df.query(logic_query)
            if len(prim_df) >0:
                for prim_ind in range(prim_df.shape[0]):
                    prim_rec = prim_df.iloc[[prim_ind]]
                    query_text = udf_glbl_update_querytext(query_text)
                    payload = {
                        "query_text": query_text,  # Update the query text here
                        "form_index": str(prim_rec['form_index'].values[0]),
                        "modif_dts": str(pd.to_datetime(prim_rec['modif_dts'].values[0])),
                        "stg_ck_event_id": int(prim_rec['ck_event_id']),
                        "relational_ck_event_ids": [],
                        "confid_score": 1,
                    }
                    payload_records.append(payload)
    return payload_records
	
	