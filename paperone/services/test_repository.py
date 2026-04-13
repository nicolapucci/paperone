from services.postgres_engine import engine
from services.logger import logger
from sqlalchemy import text
from services.product_repository import ProductRepository


count_tests_query = """
select count(
	case when et.rc_version = 0 and ( a.automatic_name = 'RANOREX AUTOMATIC' OR a.automatic_name = 'FULL AUTOMATIC') then 1 end) as \"automatic tests\",
	count(case when et.rc_version = 0 then 1 end) as \"planned tests\",
        count(case when et.rc_version != 0 then 1 end) as \"executed tests\",
	md.version
from master_tests as mt
	join automatics as a on mt.automatic_id = a.id
	join master_domains as md on mt.master_domain_id = md.id
	join env_test_details as etd on mt.env_test_details_id = etd.id
        join event_tests as et on et.master_test_id = mt.id
where md.version not like '%aborted%'
	and mt.deleted_at is null
	and not (md.product_name like '%Kalliope PBX OMNIA%' and etd.env_description like '%MONOTENANT%')
	and not (md.product_name like '%Kalliope PBX LEGACY%' and etd.env_description like '%MULTITENANT%')
group by md.version;
"""
FTE = (40*3 + 32* 2)
def okr3():
    with engine.connect() as conn:
        results = conn.execute(text(count_tests_query)).all()
    res = []
    changelog_releases = ProductRepository.changelog_releases()
    rc0_releases = ProductRepository.rc0_releases()
    for result in results:
        a_count,p_count,e_count,fw = result
        changelog_release =changelog_releases[fw] if fw in changelog_releases.keys() else None

        rc0_release = rc0_releases[fw] if fw in rc0_releases.keys() else None
        date = changelog_release if changelog_release is not None else rc0_release
        tests_over_fte = e_count / (FTE*2)
        automated_percentage = a_count / p_count
        if date is not None:
            res.append({"tests over fte":tests_over_fte,"automated percentage":automated_percentage,"firmware version":fw,"date":date})
        else:
            logger.debug(f"skipping fw:{fw}")
    return res
