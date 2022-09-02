import os
import argparse
from math import ceil
import pprint
import multiprocessing as mp
import pandas as pd
from yargy import Parser

from rules import (
    GREETING,
    INTRODUCE,
    COMPANY,
    BYE,
    get_type,
    make_task
)


# Принцип работы:
# Сначала выполняется поиск сущностей вначале диалога (только в первых n_first_rows рядах диалога, потому что, если поздоровался в середине, это на мой взгляд, все равно не выполнение) одновременно: приветствия, представления, названия компании (GREETING, INTRODUCE, COMPANY соответственно). Если сущность нашлась, то она удаляется из списка правил парсера, чтобы он не выполнял лишнюю работу и работал дальше быстрее. Когда n_first_rows рядов закончились или все три сущности найдены, парсер переходит к концу диалога n_last_rows и выполняет поиск, где менеджер попрощался (сущность BYE).
class DialogParser:
    def __init__(self, n_first_rows:int = 4, n_last_rows:int = 4):

        self.n_first_rows=n_first_rows
        self.n_last_rows=n_last_rows

        self.dialogs = []

    def parse_part_dialog(self,se:pd.Series):

        for i_row, text in se.iteritems():

            task = self.current_task()
            if not task:
                break

            yargy_parser = Parser(task)

            for match in yargy_parser.findall(text[0].lower()+text[1:]):
                self.extract(i_row, match)

    def parse(self, df:pd.DataFrame):

        # iterating on all dialogs
        for dlg_id in df["dlg_id"].unique():

            self.dialogs.append({"dlg_id":dlg_id, "greet_and_bye":False})

            # starting rows of dialog 
            start_dialog_df = df.loc[(df['role']=='manager') & (df['dlg_id']==dlg_id),'text'][:self.n_first_rows]

            self.rules = [GREETING,INTRODUCE,COMPANY]
            self.parse_part_dialog(start_dialog_df)

            # ending rows of dialog 
            end_dialog_df = df.loc[(df['role']=='manager') & (df['dlg_id']==dlg_id),'text'][-self.n_last_rows:]

            self.rules = [BYE]
            self.parse_part_dialog(end_dialog_df)

            # check the manager greeted and said goodbye to the client
            self.check_greet_and_bye()

        return self.dialogs
        
    def reset(self):
        self.dialogs = []

    def current_task(self):
        return make_task(self.rules)

    def extract(self, i_row:int, match):

        full_text = ' '.join([t.value for t in match.tokens])
        
        dialog = self.dialogs[-1]

        value = match.fact.value
        type_subtask = get_type(value)

        if type_subtask == 'greeting':
            dialog['greeting_row'] = i_row
            dialog['greeting_text'] = full_text
        elif type_subtask == 'introduce':
            dialog['introduce_row'] = i_row
            dialog['introduce_text'] = full_text
            second = value.second if value.second else ''
            dialog['manager_name'] = value.first + second
        elif type_subtask == 'company':
            dialog['company_row'] = i_row
            dialog['company_name'] = value.name
        elif type_subtask == 'bye':
            dialog['bye_row'] = i_row
            dialog['bye_text'] = full_text
        else:
            raise TypeError(f'find unknown fact: {value}')

    def check_greet_and_bye(self):
        if self.dialogs[-1].get('greeting_row') and self.dialogs[-1].get('bye_row'):
            self.dialogs[-1]['greet_and_bye'] = True


def get_dialogs(df: pd.DataFrame, n_jobs:int):
    """
    distribute dialogs equally among workers
    """

    dlg_ids = df["dlg_id"].unique()
    num_dialogs = len(dlg_ids)

    step = ceil(num_dialogs/n_jobs)

    for i in range(0,num_dialogs,step):
        yield df.loc[df["dlg_id"].isin(dlg_ids[i:i+step])]

def parallel_parse(df: pd.DataFrame, n_first:int = 4, n_last:int = 4,n_jobs:int = -1):
    """
    create n_proc parsers and process dialog dataset
    """
    if n_jobs == -1:
        n_jobs = os.cpu_count()

    results = []
    with mp.Pool(n_jobs) as pool:
        for data in pool.imap_unordered(DialogParser(n_first, n_last).parse, get_dialogs(df, n_jobs)):
                results.extend(data)

    return results

def extend_data(df:pd.DataFrame, results:list):
    """
    add to test data columns with new extracted info about managers
    """
    temp = {'greeting': {}, 'manager': {}, 'company': {}, 'bye': {}}
    for dlg in results:

        if dlg.get('greeting_row'):
            temp['greeting'][int(dlg.get('greeting_row'))] = dlg['greeting_text']
        if dlg.get('introduce_row'):
            temp['manager'][int(dlg.get('introduce_row'))] = dlg['manager_name']
        if dlg.get('company_row'):
            temp['company'][int(dlg.get('company_row'))] = dlg['company_name']        
        if dlg.get('bye_row'):
            temp['bye'][int(dlg.get('bye_row'))] = dlg['bye_text']
    
    return df.join(pd.DataFrame(temp),how='left')


def parse_args():
    parser = argparse.ArgumentParser(description="Parser dialog entities")
    parser.add_argument("--input_file", dest="input_file",type=str, required=False, default="test_data.csv", help="file with dialogs")
    parser.add_argument("--output_file", dest="output_file",type=str, required=False, default="dialogs.txt", help="file where the results will be saved")
    parser.add_argument("--n_first", dest="n_first",type=str, required=False, default=4)
    parser.add_argument("--n_last", dest="n_last",type=int, required=False, default=4)
    parser.add_argument("--n_jobs", dest="n_jobs",type=int, required=False, default=-1)

    return parser.parse_args()

if __name__ == "__main__":

    args = parse_args()

    df = pd.read_csv(args.input_file)
    # поменял роли по смыслу диалогов
    df.role = df.role.map({'manager':'client','client':'manager'})

    results = parallel_parse(df, args.n_first, args.n_last, args.n_jobs)



    with open(args.output_file, 'wt') as f:
	    f.write(pprint.pformat(sorted(results,key=lambda item: item['dlg_id']),sort_dicts=False))

    ext_df = extend_data(df, results)
    ext_df.to_csv('extended_test_data.csv')