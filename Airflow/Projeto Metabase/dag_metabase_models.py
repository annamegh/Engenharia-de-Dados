from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'metabase',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'dagrun_timeout': timedelta(hours=2),
    'depends_on_past': False,
    'max_active_runs': 1,
}

@dag(
    'metabase_models',
    default_args=default_args,
    description='Executa os modelos gerenciais do metabase',
    schedule_interval= '0 10 * * 1-5',
    start_date=days_ago(1),
    catchup=False, 
    tags=["metabase"]
)
def metabase_dag():

    @task()
    def users_group_query():
        sql_query = """
        DROP TABLE IF EXISTS gerenciamento_metabase.modelo_grupos_e_usuarios;

        CREATE TABLE gerenciamento_metabase.modelo_grupos_e_usuarios as

            select 
            pg.id as id_grupo,
            pg."name" as nome_grupo,
            u.id as id_usuario,
            u.email as email_usuario,
            u.first_name || ' ' || coalesce(u.last_name, '') as nome_usuario

            from gerenciamento_metabase.permissions_group pg 
            left join gerenciamento_metabase.permissions_group_membership pgm on pg.id = pgm.group_id 
            left join gerenciamento_metabase.core_user u on u.id = pgm.user_id 
            where pg.id not in (1, 32, 33, 34, 35)

        """
        return sql_query
    
    def colecoes_query():
        sql_query = """
        DROP TABLE IF EXISTS gerenciamento_metabase.modelo_colecoes;

        CREATE TABLE gerenciamento_metabase.modelo_colecoes as

            select 
            c.id as id_colecao,
            c.name as nome_colecao,
            case when c."location" != '/' then 'Não' else 'Sim' end as e_colacao_principal,
            coalesce(id_colecao_principal, c.id) as id_colecao_principal,
            case when coalesce(c2.name, c.name) like 'CRMThink%' then 'CRMThink' else coalesce(c2.name, c.name) end as nome_colecao_principal,
            cast(c.created_at as  timestamp) as data_de_criacao
            
            from gerenciamento_metabase.collection c 
            left join lateral (
                select 
                (regexp_matches(c."location", '/([0-9]+)'))[1]::int AS id_colecao_principal
            ) r ON true
            left join gerenciamento_metabase.collection c2 on c2.id = id_colecao_principal
            where c.slug not like '%colecao_pessoal%' and c.slug not like '%personal_collection%' and c.archived = false

        """
        return sql_query
    
    def metabase_access_query():
        sql_query ="""
        DROP TABLE IF EXISTS gerenciamento_metabase.modelo_metabase_acessos;

        CREATE TABLE gerenciamento_metabase.modelo_metabase_acessos as
            
            
            with grupos_internos as 
            (
                select 
                id_usuario,
                id_grupo
                from gerenciamento_metabase.modelo_grupos_e_usuarios 
                where id_grupo in (2, 70) and id_origem = 1
            )
            
            select 
            rd.id as id_painel, 
            rd."name" as painel,
            c.id_colecao_principal,
            c.nome_colecao_principal,
            gu.id_usuario,
            gu.nome_usuario,
            gu.email_usuario,
            case when min(gi.id_grupo) = 2 then 'Sim' else 'Não' end as usuario_administrador,
            case when max(gi.id_grupo) = 70 then 'Sim' else 'Não' end as usuario_interno,
            date_trunc('hour', started_at) + (extract(minute FROM started_at)::int / 10) * interval '10 minute' - interval '3 hour' as data_de_acesso

            from gerenciamento_metabase.query_execution qe 
            join gerenciamento_metabase.modelo_grupos_e_usuarios  gu on gu.id_usuario = qe.executor_id
            join gerenciamento_metabase.report_dashboard rd on rd.id = qe.dashboard_id 
            left join grupos_internos gi on gi.id_usuario = qe.executor_id
            left join gerenciamento_metabase.modelo_colecoes c on c.id_colecao = rd.collection_id 
            
            where qe.context = 'dashboard' and gu.id_origem = 1 and c.id_origem = 1
            
            group by  1,2,3,4,5,6,7,10

	
        """
        return sql_query
    
    def users_query():
        sql_query ="""
        DROP TABLE IF EXISTS gerenciamento_metabase.modelo_usuarios;

        CREATE TABLE gerenciamento_metabase.modelo_usuarios as

            with permissions as (
            
                select distinct
                substring("object" FROM '/collection/([0-9]+)/')::int as id_colecao,
                group_id
                
                from gerenciamento_metabase.permissions p 
                where "object" like '/collection%'
            ),
            
            grupos_internos as (
            
                select 
                id_usuario,
                id_grupo
                from gerenciamento_metabase.modelo_grupos_e_usuarios 
                where id_grupo in (2, 70) and id_origem = 1
            )
            
            select distinct
            id_usuario,
            email_usuario,
            nome_usuario,
            nome_colecao_principal
            
            from gerenciamento_metabase.modelo_grupos_e_usuarios gu
            join permissions p on p.group_id = gu.id_grupo  
            join gerenciamento_metabase.modelo_colecoes c on p.id_colecao = c.id_colecao 
            where not exists (select * from grupos_internos gi where gu.id_usuario = gi.id_usuario) and c.id_origem = 1 and gu.id_origem = 1 and id_usuario is not null

        """
        return sql_query
    
    ##########################################################################################################################################
    
    users_group_task = SQLExecuteQueryOperator(
        task_id='users_group_task',
        conn_id='datalake_ambiente_demo',  # Nome da conexão configurada no Airflow
        sql=users_group_query(),
    )
    
    colecoes_task = SQLExecuteQueryOperator(
        task_id='colecoes_task',
        conn_id='datalake_ambiente_demo',  # Nome da conexão configurada no Airflow
        sql=colecoes_query(),
    )
    
    metabase_access_task = SQLExecuteQueryOperator(
        task_id='metabase_access_task',
        conn_id='datalake_ambiente_demo',  # Nome da conexão configurada no Airflow
        sql=metabase_access_query(),
    )
    
    users_task = SQLExecuteQueryOperator(
        task_id='users_task',
        conn_id='datalake_ambiente_demo',  # Nome da conexão configurada no Airflow
        sql=users_query(),
    )

    users_group_task >> colecoes_task >> metabase_access_task >> users_task
    
    
metabase_dag = metabase_dag()
