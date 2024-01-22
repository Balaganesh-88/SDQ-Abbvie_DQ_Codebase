"""
Rule Name: rulefn_IDRP_Check_177_V
Domains: lb_local_df
logic: LBCAT = CHEMISTRY and LBDTC <> Null and LBTESTCD <> Null  and LBORRES <> Null and LBNRIND = NULL. Query should fire.
"""

def rulefn_IDRP_Check_177_V(query_text, lb_local_df):
    payload_records = []
    prim_df = lb_local_df
    if prim_df.shape[0] > 0:
        args_dict = {
            'p_form_ls': ['LB_A_CH3_F'],
            'p_visit_ls': [],
            'logic_text': " (LBCAT.str.upper() == 'CHEMISTRY') and (LBDTC not in @null_values) and (LBTESTCD  not in @null_values) and (LBORRES  not in @null_values) and(LBNRIND.isna() or LBNRIND in @null_values)",
            'null_values': ['null', 'Null', 'NULL', 'NaN', '', ' ', 'None', 'NaT', 'np.nan', 'nan', None, np.nan],
            'refid_flag': False,
            'refid_col': []
        }
        # form, visit = args_dict['form'], args_dict['visit']
        p_form_ls = args_dict['p_form_ls']
        p_visit_ls = args_dict['p_visit_ls']
        null_values = args_dict['null_values']
        logic_query = args_dict['logic_text']
        refid_filter = args_dict['refid_flag']
        refid_col = args_dict['refid_col']
        prim_df = udf_glbl_filter_by_formid_visitid(prim_df, p_form_ls, p_visit_ls)
        if len(prim_df) > 0:
            prim_df = prim_df.query(logic_query)
        if len(prim_df) > 0:
            if refid_filter:
                prim_df = prim_df.sort_values(by='SPID', ascending=True).drop_duplicates(subset=refid_col,keep='first').sort_index().reset_index(drop=True)
            for prim_ind in prim_df.index.tolist():
                prim_rec = prim_df.loc[[prim_ind]]
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
