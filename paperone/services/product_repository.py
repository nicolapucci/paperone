from datetime import datetime
import pandas as pd
import re

tmp_release_mapper = {
    '4.0.0':datetime(2015,11,17),
    '4.0.1':datetime(2015,11,19),
    '4.0.2':datetime(2015,11,27),
    '4.0.3':datetime(2015,12,11),
    '4.0.4':datetime(2015,12,23),
    '4.0.5':datetime(2016,1,15),
    '4.0.6':datetime(2016,1,29),
    '4.0.7':datetime(2016,2,15),
    '4.0.8':datetime(2016,3,4),    
    '4.0.9':datetime(2016,5,10),
    '4.0.10':datetime(2016,5,27),
    '4.1.1':datetime(2016,4,6),
    '4.1.2':datetime(2016,4,28),
    '4.1.3':datetime(2016,5,2),
    '4.1.4':datetime(2016,5,10),
    '4.1.5':datetime(2016,5,31),
    '4.1.6':datetime(2016,6,23),
    '4.1.7':datetime(2016,7,5),
    '4.2.0':datetime(2016,7,15),
    '4.2.1':datetime(2016,7,29),
    '4.2.2':datetime(2016,9,6),
    '4.2.3':datetime(2016,9,28),
    '4.2.4':datetime(2016,10,7),
    '4.2.5':datetime(2016,11,14),
    '4.2.6':datetime(2016,12,15),
    '4.2.7':datetime(2017,2,16),
    '4.3.0':datetime(2016,8,9),
    '4.3.1':datetime(2016,9,5),
    '4.3.2':datetime(2016,9,13),
    '4.3.3':datetime(2016,9,30),
    '4.3.4':datetime(2016,10,17),
    '4.3.5':datetime(2016,11,8),
    '4.3.6':datetime(2016,11,28),
    '4.3.7':datetime(2017,1,10),
    '4.3.8':datetime(2017,2,6),
    '4.3.9':datetime(2017,3,9),
    '4.3.10':datetime(2017,3,17),
    '4.3.11':datetime(2017,4,13),
    '4.4.0':datetime(2017,5,3),
    '4.4.1':datetime(2017,5,29),
    '4.4.2':datetime(2017,8,31),
    '4.5.0':datetime(2017,5,22),
    '4.5.1':datetime(2017,5,17),
    '4.5.2':datetime(2017,6,29),
    '4.5.3':datetime(2017,7,3),
    '4.5.4':datetime(2017,8,30),
    '4.5.5':datetime(2017,10,25),
    '4.5.6':datetime(2017,11,29),
    '4.5.7':datetime(2017,1,19),
    '4.5.8':datetime(2018,3,12),
    '4.5.9':datetime(2018,5,21),
    '4.5.10':datetime(2018,5,29),
    '4.5.11':datetime(2018,6,6),
    '4.5.12':datetime(2018,6,25),
    '4.5.13':datetime(2018,7,2),
    '4.5.14':datetime(2018,7,13),
    '4.5.15':datetime(2018,7,16),
    '4.5.16':datetime(2018,7,24),
    '4.5.17':datetime(2018,7,31),
    '4.6.0':datetime(2018,9,19),
    '4.6.1':datetime(2018,10,23),
    '4.6.2':datetime(2019,1,16),
    '4.7.0':datetime(2018,10,2),
    '4.7.1':datetime(2018,10,11),
    '4.7.2':datetime(2018,10,25),
    '4.7.3':datetime(2018,11,27),
    '4.7.4':datetime(2018,12,11),
    '4.7.5':datetime(2019,1,17),
    '4.7.6':datetime(2019,1,31),
    '4.7.7':datetime(2019,2,18),
    '4.7.8':datetime(2019,2,25),
    '4.7.9':datetime(2019,3,7),
    '4.7.10':datetime(2019,2,28),
    '4.7.11':datetime(2019,4,4),
    '4.7.12':datetime(2019,6,6),
    '4.7.13':datetime(2019,7,9),
    '4.7.14':datetime(2019,7,18),
    '4.7.15':datetime(2019,7,22),
    '4.7.16':datetime(2019,8,8),
    '4.7.17':datetime(2019,9,19),
    '4.8.0':datetime(2019,12,2),
    '4.8.1':datetime(2019,12,3),
    '4.8.2':datetime(2019,12,23),
    '4.8.3':datetime(2020,1,13),
    '4.8.4':datetime(2020,4,30),
    '4.8.5':datetime(2020,5,18),
    '4.9.0':datetime(2019,10,31),
    '4.9.1':datetime(2019,11,12),
    '4.9.2':datetime(2019,12,17),
    '4.9.3':datetime(2019,12,19),
    '4.9.4':datetime(2020,1,28),
    '4.9.5':datetime(2020,2,3),
    '4.9.6':datetime(2020,2,27),
    '4.9.7':datetime(2020,3,6),
    '4.9.8':datetime(2020,4,26),
    '4.9.9':datetime(2020,5,6),
    '4.10.0':datetime(2020,7,21),
    '4.10.1':datetime(2020,10,27),
    '4.10.2':datetime(2020,11,17),
    '4.11.0':datetime(2020,6,5),
    '4.11.1':datetime(2020,7,3),
    '4.11.2':datetime(2020,8,12),
    '4.11.3':datetime(2020,10,10),
    '4.11.4':datetime(2020,10,23),
    '4.11.5':datetime(2020,11,17),
    '4.11.6':datetime(2020,11,19),
    '4.11.7':datetime(2020,12,1),
    '4.11.8':datetime(2020,12,22),
    '4.11.9':datetime(2020,12,28),
    '4.11.10':datetime(2021,2,8),
    '4.11.11':datetime(2021,3,9),
    '4.11.12':datetime(2021,3,17),
    '4.12.0':datetime(2021,4,28),
    '4.12.1':datetime(2021,7,16),
    '4.13.0':datetime(2021,6,23),
    '4.13.1':datetime(2021,9,13),
    '4.13.2':datetime(2021,9,29),
    '4.13.3':datetime(2021,12,4),
    '4.13.4':datetime(2021,12,23),
    '4.13.5':datetime(2021,12,30),
    '4.13.6':datetime(2022,2,14),
    '4.13.7':datetime(2022,4,5),
    '4.13.8':datetime(2022,5,5),
    '4.14.0':datetime(2022,5,27),
    '4.14.1':datetime(2023,1,26),
    '4.15.0':datetime(2022,6,8),
    '4.15.1':datetime(2022,9,29),
    '4.15.2':datetime(2022,10,4),
    '4.15.3':datetime(2022,11,25),
    '4.15.4':datetime(2023,3,12),
    '4.15.5':datetime(2023,4,14),
    '4.15.6':datetime(2023,5,24),
    '4.15.7':datetime(2023,7,19),
    '4.15.8':datetime(2023,10,12),
    '4.15.9':datetime(2023,10,27),
    '4.15.10':datetime(2023,11,13),
    '4.15.11':datetime(2024,1,5),
    '4.15.12':datetime(2024,1,25),
    '4.15.13':datetime(2024,2,1),
    '4.15.14':datetime(2024,2,15),
    '4.16.0':datetime(2024,2,22),
    '4.16.1':datetime(2024,4,5),
    '4.16.2':datetime(2024,5,22),
    '4.17.0':datetime(2024,6,5),
    '4.17.1':datetime(2024,7,22),
    '4.17.2':datetime(2024,9,2),
    '4.17.3':datetime(2024,11,7),
    '4.17.4':datetime(2024,12,11),
    '4.17.5':datetime(2024,12,17),
    '4.17.6':datetime(2025,1,31),
    '4.17.7':datetime(2025,3,12),
    '4.17.8':datetime(2025,3,20),
    '4.17.9':datetime(2025,5,6),
    '4.17.10':datetime(2025,5,20),
    '4.17.11':datetime(2025,6,4),
    '4.17.12':datetime(2025,10,3),
    '4.17.13':datetime(2025,12,10),
}

build_release_sheet_name = "License_Server"
changelog_sheet_name = "Changelog"
time_to_test_release_path = "resources/Modello_TCoE_Report_OKR-2_Iniziativa-Tempo_Test_Release.xlsx"


class ProductRepository:

    def rc0_releases():
        return tmp_release_mapper

    def get_tempo_di_esecuzione_medio():
        build_release, changelog = get_time_to_test_release_data()
        
        first_build_release = {}

        for item in build_release:
            version = item['Build']
            release = item['release_date']

            if not  version in first_build_release:
                first_build_release[version] = release
            elif release < first_build_release[version]:
                first_build_release[version] = release

        montly_stat = {}
        montly_stat_excluding_short_tests = {}

        for version,date in changelog.items():

            rc0_release = first_build_release[version]
            days_passed = (date - rc0_release).days

            date = datetime(
                year=date.year,
                month=date.month,
                day=1
            )
            if days_passed >7:
                if date in montly_stat_excluding_short_tests:
                    montly_stat_excluding_short_tests[date]={
                        "releases":montly_stat_excluding_short_tests[date]["releases"]+1,
                        "test_days":montly_stat_excluding_short_tests[date]["test_days"]+days_passed
                    }
                else:
                    montly_stat_excluding_short_tests[date]={
                        "releases":1,
                        "test_days":days_passed
                    }

            if date in montly_stat:
                montly_stat[date]={
                    "releases":montly_stat[date]["releases"]+1,
                    "test_days":montly_stat[date]["test_days"]+days_passed,   
                }
                montly_stat[date]["avg_test_duration"]=montly_stat[date]["test_days"] / montly_stat[date]["releases"]
            else:
                montly_stat[date]={
                    "releases":1,
                    "test_days":days_passed,
                    "avg_test_duration":days_passed
                }

        def recap(montly_stat):
            date = datetime( #prendo solo gli ultimi 2 anni
                year=datetime.today().year -2,
                month=datetime.today().month,
                day=1
            )
            recap = {}
            while date < datetime.today():
                if date not in montly_stat:
                    recap[date]={
                        "releases":0,
                        "test_days":0
                    }
                else:
                    recap[date]=montly_stat[date]
                year = date.year if date.month<12 else date.year+1
                month = date.month + 1 if date.month<12 else 1
                date = datetime(
                    year=year,
                    month=month,
                    day=1
                )

            
            prova = {}
            detailed_stats = []
            generic_stats = []
            
            for date,data in recap.items():
                firmware_releases = 0
                test_days = 0

                for i in range (1,7):
                    month = date.month
                    year = date.year


                    year = year if month>i else year -1
                    month = month-i if month>i else month+12-i

                    new_date = datetime(
                        year=year,
                        month=month,
                        day=1
                    )

                    if new_date in recap:
                        month_data = recap[new_date].copy()
                        month_data['Date']=date
                        month_data['Event Date']=new_date
                        detailed_stats.append(month_data)

                        test_days += recap[new_date]["test_days"]
                        firmware_releases += recap[new_date]["releases"]

                media_mobile = test_days / firmware_releases if firmware_releases > 0 else 0
                recap[date]["media_mobile"] = media_mobile
                generic_stats.append({"date":date,"media_mobile":media_mobile})
                prova[date] = recap[date]["media_mobile"]
            
            return generic_stats,detailed_stats

        [generic_stats,detailed_stats]= recap(montly_stat)
        return {
            "generic_stats":generic_stats,
            "partial":recap(montly_stat_excluding_short_tests)[0],
            "detailed_recap":detailed_stats
        }



def helper_clean_version(version_str):
    if isinstance(version_str, str):
        splits = re.split('-', version_str)
        return splits[0].strip()
    return version_str


def helper_parse_date(date_str):
    if isinstance(date_str, pd.Timestamp):
        return date_str.date()
    return pd.to_datetime(date_str).date()

def helper_parse_changelog_date(date_str):
    [day,month,year] = re.split('/',date_str,maxsplit=2)
    return helper_parse_date( f"{month}/{day}/{year}")

    
def get_time_to_test_release_data():
    build_release_df = pd.read_excel(time_to_test_release_path, sheet_name=build_release_sheet_name)
    changelog_df = pd.read_excel(time_to_test_release_path, sheet_name=changelog_sheet_name)

    build_release_df.columns = build_release_df.columns.str.strip()
    changelog_df.columns = changelog_df.columns.str.strip()
    
    build_release_df = pd.DataFrame({
        "Build": build_release_df.iloc[:,0].apply(helper_clean_version),
        "release_date": build_release_df['release_date'].apply(helper_parse_date)
    })
   
    build_release_dict = build_release_df.to_dict(orient='records')

    changelog_df = pd.DataFrame({
        "Version": changelog_df.iloc[:, 1],
        "release_date": changelog_df.iloc[:, 3].apply(helper_parse_changelog_date)
    })  
    changelog_dict = dict(zip(
        changelog_df["Version"],
        changelog_df["release_date"]
    ))

    return build_release_dict, changelog_dict
