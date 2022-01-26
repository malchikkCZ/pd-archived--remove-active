import pandas as pd


def read_source_xlsx(filename):
    '''Read source xls file into separate dataframes'''
    xls = pd.ExcelFile(filename)
    data = {}
    for sheet in xls.sheet_names:
        data[sheet] = pd.read_excel(xls, sheet)
    return data


def build_output_xlsx(df):
    '''Write output to xls to import via Matrixify'''
    xls_writer = pd.ExcelWriter('output.xlsx')
    df[['ID', 'Command', 'Title']].to_excel(xls_writer, 'Pages', index=False)
    df[['Path', 'Target']].to_excel(xls_writer, 'Products', index=False)
    xls_writer.save()


def get_reduced_df(df, column, value):
    '''Reduce source dataframe of pages to only archived products'''
    mask = df.apply(lambda row: row[column] == value, axis=1)
    return df[mask]


def is_product_active(value, compared_df):
    '''Reduce dataframe of archived products to those also in active products dataframe'''
    return value in compared_df['Variant Metafield: mf_pvp.MKT_ID_SHOPSYS [number_integer]'].values


def main():
    source = read_source_xlsx('source.xlsx')

    products = source['Products']
    archived = get_reduced_df(source['Pages'], 'Template Suffix', 'archived-goods')
    archived['mask'] = archived.apply(lambda row: is_product_active(row['Metafield: mf_pg_ap.Shpsys_ID [integer]'], products), axis=1)

    selected = pd.merge(
        left=get_reduced_df(archived, 'mask', True),
        right=products[['Variant Metafield: mf_pvp.MKT_ID_SHOPSYS [number_integer]', 'Handle']],
        left_on='Metafield: mf_pg_ap.Shpsys_ID [integer]',
        right_on='Variant Metafield: mf_pvp.MKT_ID_SHOPSYS [number_integer]',
        how='left'
    )

    selected = selected.rename(columns={'Handle_x': 'Path', 'Handle_y': 'Target'})

    selected['Path'] = selected.apply(lambda row: f'/pages/{row["Path"]}', axis=1)
    selected['Target'] = selected.apply(lambda row: f'/products/{row["Target"]}', axis=1)
    selected['Command'] = 'DELETE'

    build_output_xlsx(selected)


if __name__ == '__main__':
    main()