import utilities.common_functions as common_functions

def save_deid_annotations(annotation_records):
    for blobid, record in annotation_records.items():
        common_functions.save_deid_annotation(blobid, record['hdcpupdatedate'], str(record['annotated_note']))


def save_unreviewed_annotations(annotation_records):
    for blobid, record in annotation_records.items():
        common_functions.save_unreviewed_annotation(blobid, record['hdcpupdatedate'], str(record['annotated_note']))
