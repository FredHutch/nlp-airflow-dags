import utilities.common_hooks as common_hooks

def save_note_to_temp_storage(blobid, hdcpupdatedate, metadata_dict):

    insert_stmt = ("INSERT INTO TEMP_NOTES (HDCOrcaBlobID, HDCPUpdateDate,"
                   "CLINICAL_EVENT_ID, HDCPersonId, BLOB_CONTENTS,"
                   "SERVICE_DT_TM, INSTITUTION, EVENT_CD_DESCR) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    print("saving metadata to temp storage for {}, {}".format(blobid, hdcpupdatedate))
    annotations_db = common_hooks.get_annotations_db_hook()
    annotations_db.run(insert_stmt, parameters=(blobid,
                                                   hdcpupdatedate,
                                                   metadata_dict["clinical_event_id"],
                                                   metadata_dict["patient_id"],
                                                   metadata_dict["blob_contents"],
                                                   metadata_dict["servicedt"],
                                                   metadata_dict["instit"],
                                                   metadata_dict["cd_descr"]), autocommit=True)




def save_person_info_to_temp_storage(blobid, hdcpupdatedate, patient_data):
    insert_stmt = ("INSERT INTO TEMP_PERSON "
                  "(HDCOrcaBlobId, HDCPUpdateDate, HDCPersonId, FirstName, MiddleName, LastName) "
                  "VALUES (%s, %s, %s, %s, %s, %s)")
    print("saving person info to temp storage for {}, {}: {}".format(blobid, hdcpupdatedate, patient_data[0]))
    annotations_db = common_hooks.get_annotations_db_hook()
    annotations_db.run(insert_stmt, parameters=(blobid,
                                                   hdcpupdatedate,
                                                   patient_data[0],
                                                   patient_data[1],
                                                   patient_data[2],
                                                   patient_data[3]), autocommit=True)
