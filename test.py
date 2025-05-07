from utils.mysql_connect_funcs import get_df_tblName

df = get_df_tblName("companyDetails")
print(df.at[0,'content'])
