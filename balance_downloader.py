import pandas as pd
import requests
import yaml
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO)

with open('tasks.yaml') as f:
    tasks = yaml.safe_load(f)

df_BoT = pd.DataFrame(columns=['wallet', 'chain', 'symbol', 'price', 'units']).round(4)
df_BoP = pd.DataFrame(columns=['wallet', 'Protocol ID', 'Pools', 'Price', 'Units', 'Pool Value', 'Reward', 'R Price', 'R Units', 'Total Value']).round(4)

for task in tasks:
    logging.info(f'Collecting data from wallet: {task["name"]}')
    headers = {
        'accept': 'application/json',
        'AccessKey': task['access']
    }
    # append balance of tokens data
    chain_resp = requests.get(f'https://pro-openapi.debank.com/v1/user/used_chain_list?id={task["wallet"]}', headers=headers).json()
    for chain_item in chain_resp:
        logging.info(f'get data of chain: {chain_item["id"]}')
        data_resp = requests.get(f'https://pro-openapi.debank.com/v1/user/token_list?id={task["wallet"]}&chain_id={chain_item["id"]}&is_all=false&has_balance=true', headers=headers).json()
        for data_item in data_resp:
            df_BoT.loc[len(df_BoT)] = [task['wallet']] + [data_item[k] for k in ['chain', 'symbol', 'price', 'amount']]
    # apeend balance of pools data
    for protocol_id in task['protocol_ids']:
        logging.info(f'get data of protocol: {protocol_id}')
        protocol_resp = requests.get(f'https://pro-openapi.debank.com/v1/user/protocol?id={task["wallet"]}&protocol_id={protocol_id}', headers=headers).json()
        portfolio_item_list = protocol_resp['portfolio_item_list']
        for portfolio_item in portfolio_item_list:
            supply = portfolio_item['detail']['supply_token_list']
            if 'reward_token_list' in portfolio_item['detail'].keys():
                reward = portfolio_item['detail']['reward_token_list'][0]
            else:
                reward = defaultdict(lambda: None)
            pool_value = 0
            for supply_item in supply:
                pool_value += supply_item['price'] * supply_item['amount']
            for supply_item in supply:
                df_BoP.loc[len(df_BoP)] = [
                    task['wallet'],
                    protocol_id,
                    supply_item['symbol'],
                    supply_item['price'],
                    supply_item['amount'],
                    pool_value,
                    reward['symbol'],
                    reward['price'],
                    reward['amount'],
                    portfolio_item['stats']['asset_usd_value'],
                ]

# process data
logging.info('Processing data')
df_BoT['usd_value'] = df_BoT['price'] * df_BoT['units']
df_BoP = df_BoP.astype({
    'Price': float,
    'Units': float,
    'R Price': float,
    'R Units': float
})
df_BoP.insert(5, 'Balance', df_BoP['Price'].mul(df_BoP['Units']))
df_BoP.insert(10, 'R Value', df_BoP['R Price'].mul(df_BoP['R Units']))
df_BoP = df_BoP.fillna('')

# write excel
logging.info('write excel')
writer = pd.ExcelWriter('balances.xlsx', engine='xlsxwriter')
workbook = writer.book
merge_format = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 2})

df_BoT.to_excel(writer, index=False, sheet_name='Balance of Tokens')
df_BoP.to_excel(writer, index=False, sheet_name='Balance of Pools')
# merge cells in two sheets
for df, worksheet in zip([df_BoT, df_BoP], [writer.sheets['Balance of Tokens'], writer.sheets['Balance of Pools']]):
    for i, col in enumerate(df.columns):
        startCells = [1]
        for row in range(2, len(df) + 1):
            if (df.loc[row - 1, col] != df.loc[row - 2, col]):
                startCells.append(row)
        lastRow = len(df)
        for row in startCells:
            try:
                endRow = startCells[startCells.index(row) + 1] - 1
                if row == endRow:
                    worksheet.write(row, i, df.loc[row - 1, col], merge_format)
                else:
                    worksheet.merge_range(row, i, endRow, i, df.loc[row - 1, col], merge_format)
            except IndexError:
                if row == lastRow:
                    worksheet.write(row, i, df.loc[row - 1, col], merge_format)
                else:
                    worksheet.merge_range(row, i, lastRow, i, df.loc[row - 1, col], merge_format)
writer.save()

logging.info('Done')
