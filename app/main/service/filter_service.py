import pandas as pd

from app.main.service.modification_service import ModifierService



operators = [['ge ', 'greaterThanOrEqual'],
             ['le ', 'lessThanOrEqual'],
             ['lt ', 'lessThan'],
             ['gt ', 'greaterThan'],
             ['ne ', 'notEqual'],
             ['eq ', 'equals'],
             ['contains '],
            # ['In range '],
             ['startsWith '],
             ['endsWith '],
             ['datestartswith ']]


def split_filter_part(filter_part):
    for operator_type in operators:
        for operator in operator_type:
            if operator in filter_part:
                name_part, value_part = filter_part.split(operator, 1)
                name = name_part[name_part.find('{') + 1: name_part.rfind('}')]

                value_part = value_part.strip()
                v0 = value_part[0]
                if (v0 == value_part[-1] and v0 in ("'", '"', '`')):
                    value = value_part[1: -1].replace('\\' + v0, v0)
                else:
                    try:
                        value = float(value_part)
                    except ValueError:
                        value = value_part

                # word operators need spaces after them in the filter string,
                # but we don't want these later
                return name, operator_type[0].strip(), value

    return [None] * 3



def update_table(params,path,page_current, page_size, sort_by, filter):
    modifier = ModifierService()

    df= pd.read_csv(path, engine="c", dtype=str, skipinitialspace=True, na_filter=False, delimiter=";")
    filtering_expressions = filter.split(' && ')
    dff = modifier.applys(params["worksheet"], params["domain_id"], df)
    for filter_part in filtering_expressions:
        col_name, operator, filter_value = split_filter_part(filter_part)

        if operator in ('lt', 'le', 'gt', 'ge'):
            # these operators match pandas series operator method names
            dff = dff.loc[getattr(pd.to_numeric(df[col_name], errors='coerce'), operator)(float(filter_value))]
        elif operator in ('eq', 'ne'):
            dff = dff.loc[getattr(dff[col_name], operator)(filter_value)]
        elif operator == 'contains':
            dff = dff.loc[dff[col_name].str.contains(filter_value)]
        elif operator == 'Starts with':
            dff = dff.loc[dff[col_name].str.startswith(filter_value)]
        elif operator == 'Ends with':
            dff = dff.loc[dff[col_name].str.endswith(filter_value)]
      #  elif operator == 'In range':
       #     dff = dff.loc[dff[col_name].str.endswith(filter_value)]
        elif operator == 'datestartswith':
            # this is a simplification of the front-end filtering logic,
            # only works with complete fields in standard format
            dff = dff.loc[dff[col_name].str.startswith(filter_value)]

    if len(sort_by):
        dff = dff.sort_values(
            [col['column_id'] for col in sort_by],
            ascending=[
                col['direction'] == 'asc'
                for col in sort_by
            ],
            inplace=False
        )

    page = int(page_current)
    size = int(page_size)
    return dff.iloc[page * size: (page + 1) * size]