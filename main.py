import pandas as pd
from utils import Matrixify


def get_reduced_df(df, column, value):
    '''Reduce source dataframe of pages to only archived products'''
    mask = df.apply(lambda row: row[column] == value, axis=1)
    return df[mask]


def is_product_active(value, compared_df):
    '''Reduce dataframe of archived products to those also in active products dataframe'''
    return value in compared_df['Variant Metafield: mf_pvp.MKT_ID_SHOPSYS [number_integer]'].values


def main():
    source = Matrixify.read_source('source.xlsx')

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

    selected = selected.rename(columns={'Handle_x': 'Handle'})

    selected['Path'] = selected.apply(lambda row: f'/pages/{row["Handle"]}', axis=1)
    selected['Target'] = selected.apply(lambda row: f'/products/{row["Handle_y"]}', axis=1)
    selected['Command'] = 'DELETE'

    schema = {
        'Pages': ['ID', 'Command', 'Handle', 'Title'],
        'Redirects': ['Path', 'Target']
    }

    Matrixify.build_output(selected, schema, 'output.xlsx')


if __name__ == '__main__':
    pd.options.mode.chained_assignment = None
    main()
