import pandas as pd
import io
from election_data_analysis import munge as m
from election_data_analysis import juris_and_munger as jm
from election_data_analysis import user_interface as ui


def strip_empties(li: list) -> list:
    # get rid of leading empty strings
    first_useful = next(idx for idx in range(len(li)) if li[idx] != '')
    li = li[first_useful:]

    # get rid of trailing empty strings
    li.reverse()
    first_useful = next(idx for idx in range(len(li)) if li[idx] != '')
    li = li[first_useful:]
    li.reverse()

    return li


def extract_items (line: str, w: int) -> list:
    """assume line ends in \n.
    drops any trailing empty strings from list"""
    item_list = [line[idx * w: (idx + 1) * w].strip() for idx in range(int((len(line)-1)/w))]
    item_list = strip_empties(item_list)
    return item_list


def process_expressvote(f_path: str, munger: jm.Munger, err: dict) -> (pd.DataFrame, dict):
    """Assumes first column of each block is ReportingUnit, last column is contest total"""
    try:
        with open(f_path,'r') as f:
            data = f.readlines()
    except Exception as exc:
        err = ui.add_error(err,'datafile_error',f'Datafile not read:\n{exc}')
        return pd.DataFrame(), err

    # get integers from munger parameters
    w = int(munger.options['column_width'])
    etlc = int(munger.options['empty_top_line_count'])
    v_t_cc = int(munger.options['last_header_column_count'])

    for i in range(etlc-1):
        # get rid of spurious top lines, leaving one blank line at top
        data.pop(0)

    df = dict()
    while len(data) > 3:
        try:
            # pop the blank line
            data.pop(0)

            # TODO allow number & interps of headers to vary?
            # get the header lines
            header_0 = data.pop(0).strip()
            header_1 = data.pop(0)
            header_line = data.pop(0)

            # get info from header line
            field_list = extract_items(header_line, w)
            first_column = field_list[0]
            last_header = field_list[1: 1 + v_t_cc]
            assert (len(field_list) - 2) % v_t_cc == 0

            header_1_list = extract_items(header_1, w * v_t_cc)

            # TODO get column with header_1 (Candidate, for Georgia)
            #  create multi-index from the header_1 and votetype, with first index 'county/county'
            #  and last index 'Total/Total'
            index_array = [
                [field_list[0]] + [y for z in [[cand] * v_t_cc for cand in header_1_list] for y in z] + ['Total'],
                [field_list[0]] + last_header * len(header_1_list) + ['Total']
            ]
            multi_index = pd.MultiIndex.from_arrays(index_array)

            # create df from next batch of lines, with that multi-index
            # find idx of next empty line
            next_empty = next(idx for idx in range(len(data)) if data[idx] == '\n')

            # create io
            vote_count_block = io.StringIO()
            vote_count_block.write(''.join(data[:next_empty]))
            vote_count_block.seek(0)

            df[header_0] = pd.read_fwf(vote_count_block,colspecs='infer',index=False)
            df[header_0].columns = multi_index

            # Drop total column at far right
            df[header_0].drop('Total',axis=1,level=1,inplace=True)

            # move header_1 & votetype info to columns
            df[header_0] = pd.melt(
                df[header_0],
                col_level=1,
                id_vars=[first_column],
                var_name='Count',
                value_name='header_2'
            ).rename(columns={first_column:'first_column'})

            # Add columns for header_0
            df[header_0] = m.add_constant_column(df[header_0],'header_0',header_0)

            # remove processed lines from data
            data = data[next_empty:]
        except Exception as exc:
            err = ui.add_error(err,"datafile_warning",f"unparsed lines at bottom of file:\n{data}")
    raw_results = pd.concat(list(df.values()))
    return raw_results, err

