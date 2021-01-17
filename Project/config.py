class Config:
    all_columns = ['iyear', 'imonth', 'iday', 'country_txt', 'region_txt', 'city', 'success', 'suicide', 'attacktype1_txt', 'targtype1_txt', 'targsubtype1_txt', 'natlty1_txt', 'gname', 'nperps', 'weaptype1_txt', 'weapsubtype1_txt', 'nkill', 'nkillus', 'nhostkid']
    classification_columns = ['country_txt', 'region_txt', 'success', 'suicide', 'attacktype1_txt', 'targtype1_txt', 'targsubtype1_txt', 'natlty1_txt', 'gname', 'nperps', 'weaptype1_txt', 'weapsubtype1_txt', 'nkill', 'nkillus', 'nhostkid']
    detailed_columns = ['iyear', 'imonth', 'iday', 'country_txt', 'region_txt', 'success', 'suicide', 'attacktype1_txt', 'targtype1_txt', 'targsubtype1_txt', 'natlty1_txt', 'gname', 'nperps', 'weaptype1_txt', 'weapsubtype1_txt', 'nkill', 'nkillus', 'nhostkid']
    clustering_columns = ['success', 'suicide']
