import pandas as pd
import io
from election_anomaly import munge as m
from election_anomaly import juris_and_munger as jm


def factor_list(li: list) -> (list, int):
    """Finds the smallest prefix list p such that li is a multiple of p"""
    prefix = list()
    multiple = 0
    n = len(li)
    for k in range(1,n+1):
        # if k is a factor of n
        if n % k == 0:
            # see if prefix of length k generates list
            prefix = li[:k]
            if prefix * int(n/k) == li:
                multiple = int(n/k)
                break
    return prefix, multiple


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


def process_expressvote(f_path: str, munger: jm.Munger, w: int, empty_top_line_count) -> pd.DataFrame:
    with open(f_path,'r') as f:
        data = f.readlines()

    for i in range(empty_top_line_count):
        # get rid of spurious top lines
        data.pop(0)

    df = dict()
    while len(data) > 3:
        # pop the blank line
        blank = data.pop(0)

        # get the contest line, candidate line and csv-header line
        contest = data.pop(0).strip()
        candidate_line = data.pop(0)
        header_line = data.pop(0)

        #  create multi-index from the candidate and votetype, with first index 'county/county'
        #  and last index 'Total/Total'
        field_list = extract_items(header_line, w)
        votetype, candidate_count = factor_list(field_list[1:-1])
        candidate_list = extract_items(candidate_line,w * len(votetype))

        index_array = [
            [field_list[0]] + [y for z in [[cand] * len(votetype) for cand in candidate_list] for y in z] + ['Total'],
            [field_list[0]] + votetype * len(candidate_list) + ['Total']
        ]
        multi_index = pd.MultiIndex.from_arrays(index_array)

        # create df from next batch of lines, with that multi-index
        # find idx of next empty line
        next_empty = next(idx for idx in range(len(data)) if data[idx] == '\n')

        # create io
        vote_count_block = io.StringIO()
        vote_count_block.write(''.join(data[:next_empty]))
        vote_count_block.seek(0)

        df[contest] = pd.read_fwf(vote_count_block,colspecs='infer',index=False)
        df[contest].columns = multi_index

        # Drop contest-total column
        df[contest].drop('Total',axis=1,level=1,inplace=True)

        # move candidate & votetype info to columns
        df[contest] = pd.melt(df[contest],col_level=1,id_vars=['County'],var_name='Count',value_name='CountItemType_raw').rename(columns={'County':'ReportingUnit_raw'})

        # Add columns for contest
        df[contest] = m.add_constant_column(df[contest],'Contest_raw',contest)

        # remove processed lines from data
        data = data[next_empty:]

        # TODO deal with any garbage at bottom of file.

    raw_results = pd.concat(list(df.values()))
    return raw_results

if __name__ == '__main__':
    f_path = '/Users/singer3/Documents/Data-Office-Mac/Georgia/2020-Primary/Georgia_20200609_Primary.txt'
    mu = jm.Munger(
        '/Users/singer3/PycharmProjects/results_analysis/src/mungers/ga_pri',
        project_root='/Users/singer3/PycharmProjects/results_analysis/src'
    )
    width = 30
    process_expressvote(f_path, mu, width)
exit()