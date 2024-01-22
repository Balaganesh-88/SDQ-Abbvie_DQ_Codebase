def editcheck_237_V(query_text, HO_V):
    prim_df = HO_V
    payload_records = []
    if len(prim_df) :
        args_dict = {
        'prim_form': ['SAE Supplemental - Investigator'],
        'prim_visit': [],
        'prim_field_1': 'HOOCCUR',

        }
        globals().update(args_dict)

        prim_df = udf_glbl_filter_by_form_visit(prim_df, prim_form, prim_visit)
        if len(prim_df):
            p_flag, prim_df = udf_glbl_null_check(prim_df, prim_field_1, '==')
            if len(prim_df) :
                for prim_ind in range(prim_df.shape[0]):
                    prim_rec = prim_df.iloc[[prim_ind]]


                    payload = {
                            "query_text": query_text, #Update the query text here
                            "form_index": str(prim_rec['form_index'].values[0]),
                            "modif_dts": str(pd.to_datetime(prim_rec['modif_dts'].values[0])),
                            "stg_ck_event_id": int(prim_rec['ck_event_id']),
                            "relational_ck_event_ids" : [],
                            "confid_score": 1,
                        }
                    payload_records.append(payload)
    return payload_records