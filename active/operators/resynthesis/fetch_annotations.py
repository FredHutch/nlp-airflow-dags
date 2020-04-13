import utilities.common_variables as common_variables
import utilities.common_hooks as common_hooks


def get_resynth_ready_annotations_since_date(update_date_from_last_run):
    '''
    get list of annotations from the ANNOTATION_TABLE since update_date_from_last_run that are either:
       -BRAT_REVIEWED_ANNOTATION_TYPE
       -REVIEW_BYPASSED_ANNOTATION_TYPE
    and are ready for resynthesis.

    :param update_date_from_last_run:
    :return: list of dicts representing rows returned:
             {'date_created': row[0],
             'count': row[1]}
    '''
    # get last update date from source since last successful run
    # then pull record id with new update date from source
    src_select_stmt = ("SELECT date_created, count(*) "
                      "FROM {table} "
                      "WHERE date_created >= %s "
                      "AND (category = %s OR category = %s) "
                      "GROUP BY date_created ".format(table=common_variables.ANNOTATION_TABLE))

    annotations_list = []
    for row in common_hooks.ANNOTATIONS_DB.get_records(src_select_stmt,
                                             parameters=(update_date_from_last_run,
                                                         common_variables.BRAT_REVIEWED_ANNOTATION_TYPE,
                                                         common_variables.REVIEW_BYPASSED_ANNOTATION_TYPE)):
        annotations_list.append({'date_created': row[0], 'count': row[1]})

    return annotations_list