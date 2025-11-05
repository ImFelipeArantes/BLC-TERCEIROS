import customtkinter as ctk
import pandas as pd
import warnings
from unidecode import unidecode
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')
# pd.set_option('display.max_columns', None)
# pd.set_option('future.no_silent_downcasting', True)
from datetime import datetime
now = datetime.now().strftime('%d_%m_%Y')

janela = ctk.CTk()

engine = create_engine('mysql+pymysql://viabilidade:senha_segura123#@10.0.15.243:3306/desenvolvimento_viabilidade')



def arquivo_entrada():
    global arquivo_entrada_sevs_teia, sevs_teia, modelo_mapinfo
    arquivo_entrada_sevs_teia = ctk.filedialog.askopenfilename(title='Abrir arquivo de extração do TEIA')
    sevs_teia = pd.read_excel(arquivo_entrada_sevs_teia)
    modelo_mapinfo = pd.read_excel('./arquivos/arquivo_modelo.xlsx')
    modelo_mapinfo[['SEV', 'LAT', 'LONG']] = sevs_teia[['SEV', 'LATITUDE', 'LONGITUDE']]
    

    button_browse = ctk.CTkButton(janela,text="Arquivo TEIA",height=20,width=35,corner_radius=8,fg_color='green',hover_color='blue', command=arquivo_entrada)
    button_browse.place(x=10,y=100)




def gerar_arq_mapinfo():
    global custo_medio, provedores,valores_, estacoes, ids_provedores,precos_vtal,provedores_nuvem
    custo_medio = pd.read_excel('./arquivos/AMOSTRA_CUSTO_MEDIANO.xlsx')
    try:
        # provedores = pd.read_sql('SELECT * FROM localidades_terceiros_bl', engine)
        valores_ = pd.read_sql('SELECT * FROM valores_terceiros_internet', engine)
        valores_.to_excel('valores_terceiros_internet.xlsx',index=False)
    except:
        print('NAO FOI POSSIVEL CONECTAR NO BANCO DE DADOS')
        valores_ = pd.read_excel('./arquivos/valores_terceiros_internet.xlsx')
        pass
    modelo_mapinfo.fillna('').to_excel(f'SAIDAS/arquivo_mapinfo_{now}.xlsx',index=False)

    ###### SUBSTITUI VOGEL PELA ALGAR ##################
    # for i, v in provedores.iterrows():
    #     if v.PROVEDOR == 'VOGEL':
    #         provedores.at[i,'PROVEDOR'] = 'ALGAR TELECOM'
    #     elif v.PROVEDOR == 'MOBWIRE':
    #         provedores.at[i,'PROVEDOR'] = 'GIGA+'

    for i, v in valores_.iterrows():
        if v.PROVEDOR == 'VOGEL':
            valores_.at[i,'PROVEDOR'] = 'ALGAR TELECOM'
        elif v.PROVEDOR == 'MOBWIRE':
            valores_.at[i,'PROVEDOR'] = 'GIGA+'

        
    provedores_nuvem = pd.read_excel('./arquivos/provedores_nuvem.xlsx')

    # provedores = provedores[(provedores.PROVEDOR.isin(provedores_nuvem.PROVEDOR.values.tolist()))]

    valores_ = valores_[(valores_.PROVEDOR.isin(provedores_nuvem.PROVEDOR.values.tolist()))]

    for i, v in valores_.iterrows():
        try:
            valores_.at[i,'VEL'] = int(v.VELOCIDADE[:-1])
        except:
            valores_.at[i,'VEL'] = 0
    estacoes = pd.read_excel('./arquivos/estacoes_entregas.xlsx')
    ids_provedores = pd.read_excel('./arquivos/ids_provedores_bl.xlsx')
    precos_vtal = pd.read_excel('./arquivos/precos_vtal.xlsx')

    for i,v in ids_provedores.iterrows():

        ids_provedores.at[i,'EMPRESA'] = v['EMPRESA'].strip()
        ids_provedores.at[i,'PROVEDOR'] = v['PROVEDOR'].strip()
        ids_provedores.at[i,'UF'] = v['UF'].strip()

    titulo_mapinfo= ctk.CTkLabel(janela,text='Selecione o arquivo de retorno pos Mapinfo')
    titulo_mapinfo.place(x=10,y=130)

    button_mapinfo = ctk.CTkButton(janela,text="Arquivo Mapinfo",height=20,width=35,corner_radius=8,fg_color='grey',hover_color='blue', command=arquivo_retorno)
    button_mapinfo.place(x=10,y=160)
    
    button_gera_mapinfo = ctk.CTkButton(janela,text="Gerar Arquivo para mapinfo",height=20,width=35,corner_radius=8,fg_color='green',hover_color='blue', command=gerar_arq_mapinfo)
    button_gera_mapinfo.place(x=10,y=200)

def arquivo_retorno():
    global arquivo_retorno_map_info,retorno_mapinfo,fechamento,inviaveis

    MESES = int(meses.get())

    arquivo_retorno_map_info = ctk.filedialog.askopenfilename(title='Abrir arquivo de retorno MAPINFO')
    
    inviaveis = pd.DataFrame(columns=['SEV','MOTIVO'])

    retorno_mapinfo = pd.read_excel(arquivo_retorno_map_info).fillna('NOK')

    retorno_mapinfo.columns = modelo_mapinfo.columns.tolist()

    for i, v in retorno_mapinfo.iterrows():
        retorno_mapinfo.at[i,'ID_SEV'] = sevs_teia[sevs_teia.SEV == v.SEV].ID_ANALISE.values[0]
        retorno_mapinfo.at[i,'CNL'] = sevs_teia[sevs_teia.SEV == v.SEV].CNL.values[0]
        retorno_mapinfo.at[i,'UF'] = sevs_teia[sevs_teia.SEV == v.SEV].UF.values[0]
        retorno_mapinfo.at[i,'FASE'] = sevs_teia[sevs_teia.SEV == v.SEV].FASE.values[0]
        if sevs_teia[sevs_teia.SEV == v.SEV].VELOCIDADE.values[0][-4] == 'G':
            retorno_mapinfo.at[i,'VELOCIDADE'] = int(sevs_teia[sevs_teia.SEV == v.SEV].VELOCIDADE.values[0][:-4])*1000
        else:
            retorno_mapinfo.at[i,'VELOCIDADE'] = int(sevs_teia[sevs_teia.SEV == v.SEV].VELOCIDADE.values[0][:-4])

    retorno_mapinfo[['STATUS_SHOPPING','STATUS_NUVEM','PROVEDOR_CM','INSTALACAO_PROVEDOR_CM','MENSALIDADE_PROVEDOR_CM','ESCOLHA_PROVEDOR_FINAL','MOTIVO_ESCOLHA']] = ''
    retorno_mapinfo[['INSTALACAO_PROVEDOR_CM','MENSALIDADE_PROVEDOR_CM','INSTALACAO_PROVEDOR_FINAL','MENSALIDADE_PROVEDOR_FINAL']] = 0    

    for p in provedores_nuvem.PROVEDOR.values.tolist():
        retorno_mapinfo[[f'INSTALACAO_PROVEDOR_{p}',f'MENSALIDADE_PROVEDOR_{p}']] = 0

    for i, v in retorno_mapinfo.iterrows():
        print(round((i/len(retorno_mapinfo)*100),2),end="\r")

        valores =  valores_[valores_.SIGLA_MUNICIPIO == v.CNL]
        if v['RESTRICAO_SHOPPING'] != 'NOK':                 
            retorno_mapinfo.at[i,f'STATUS_SHOPPING'] = 'INVIAVEL RESTRICAO SHOPPING'
        else:
            retorno_mapinfo.at[i,f'STATUS_SHOPPING'] = 'SEM RETRICAO DE SHOPPING'
            
            
        for p in provedores_nuvem.PROVEDOR.values.tolist():


            if p == 'VTAL':
                if v[p] == 'NOK':
                    if v['NUVEM_GENERICA'] == 'NOK':
                        if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} INVIAVEL POR NUVEM E FORA DE NUVEM GENERICA'
                        else:
                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} INVIAVEL POR NUVEM E FORA DE NUVEM GENERICA'
                    else:
                        if len(custo_medio[custo_medio.CNL == v.CNL]) > 0:
                            instalacao_cm = custo_medio[custo_medio.CNL == v.CNL][f'TERCEIROS INTERNET ASSIMÉTRICA INST - ATÉ {int(v.VELOCIDADE)}M'].values[0]
                            mensalidade_cm = custo_medio[custo_medio.CNL == v.CNL][f'TERCEIROS INTERNET ASSIMÉTRICA MENSAL - ATÉ {int(v.VELOCIDADE)}M'].values[0]
                            retorno_mapinfo.at[i,'PROVEDOR_CM'] = 'GENERICO - BANDA LARGA'
                            retorno_mapinfo.at[i,'INSTALACAO_PROVEDOR_CM'] = instalacao_cm
                            retorno_mapinfo.at[i,'MENSALIDADE_PROVEDOR_CM'] = mensalidade_cm
                            if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} INVIAVEL POR NUVEM E DENTRO DE NUVEM GENERICA'
                            else:
                                retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} INVIAVEL POR NUVEM E DENTRO DE NUVEM GENERICA'
                else:
                    
                    # contrato = provedores[(provedores.PROVEDOR == p) & (provedores.SIGLA_LOC == v.CNL)].CONTRATO.values[0]
                    preco_cnl_vtal = precos_vtal[precos_vtal.CNL == v.CNL]

                    # valor = valores[(valores.contrato == contrato) & valores.obs.str.contains('COMPARTILHADO')]
                    if len(preco_cnl_vtal[(preco_cnl_vtal.VELOCIDADE == v.VELOCIDADE) & (preco_cnl_vtal.PRAZO == 12)]) <= 0:
                        valor = preco_cnl_vtal[(preco_cnl_vtal.VELOCIDADE > v.VELOCIDADE) & (preco_cnl_vtal.PRAZO == 12)].sort_values(by=['VELOCIDADE'],ascending=True)
                        if len(valor) > 0:
                            ##### PRECO VTAL
                            retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.INSTALACAO.values[0]
                            retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.MENSALIDADE.values[0]
                            if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA/VELOCIDADE CONSIDERADA {int(valor.VELOCIDADE.values[0])}'
                            else:
                                retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA/VELOCIDADE CONSIDERADA {int(valor.VELOCIDADE.values[0])}'
                        else:
                            retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = 0
                            retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = 0
                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'INVIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA'
                    elif len(preco_cnl_vtal[(preco_cnl_vtal.VELOCIDADE == v.VELOCIDADE) & (preco_cnl_vtal.PRAZO == 24)]) <= 0:
                        valor = preco_cnl_vtal[(preco_cnl_vtal.VELOCIDADE > v.VELOCIDADE) & (preco_cnl_vtal.PRAZO == 24)].sort_values(by=['VELOCIDADE'],ascending=True)
                        if len(valor) > 0:
                            retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.INSTALACAO.values[0]
                            retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.MENSALIDADE.values[0]
                            if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA/VELOCIDADE CONSIDERADA {int(valor.VELOCIDADE.values[0])}'
                            else:
                                retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA/VELOCIDADE CONSIDERADA {int(valor.VELOCIDADE.values[0])}'
                        else:
                            retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = 0
                            retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = 0
                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'INVIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA'
                    else:
                        valor = preco_cnl_vtal[(preco_cnl_vtal.VELOCIDADE == v.VELOCIDADE) & (preco_cnl_vtal.PRAZO == 12)]
                        retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.INSTALACAO.values[0]
                        retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.MENSALIDADE.values[0]
                        if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
                        else:
                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
            
            # elif  p == 'DESKTOP':
            #     if v[p] == 'NOK':
            #         if v['NUVEM_GENERICA'] == 'NOK':
            #             if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
            #                 retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} INVIAVEL POR NUVEM E FORA DE NUVEM GENERICA'
            #             else:
            #                 retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} INVIAVEL POR NUVEM E FORA DE NUVEM GENERICA'
            #         else:
            #             if len(custo_medio[custo_medio.CNL == v.CNL]) > 0:
            #                 provedor_cm = custo_medio[custo_medio.CNL == v.CNL][f'PROVEDOR_{int(v.VELOCIDADE)}M'].values[0]
            #                 instalacao_cm = custo_medio[custo_medio.CNL == v.CNL][f'INST_{int(v.VELOCIDADE)}M'].values[0]
            #                 mensalidade_cm = custo_medio[custo_medio.CNL == v.CNL][f'MENSAL_{int(v.VELOCIDADE)}M'].values[0]
            #                 retorno_mapinfo.at[i,'PROVEDOR_CM'] = provedor_cm
            #                 retorno_mapinfo.at[i,'INSTALACAO_PROVEDOR_CM'] = instalacao_cm
            #                 retorno_mapinfo.at[i,'MENSALIDADE_PROVEDOR_CM'] = mensalidade_cm
            #                 if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
            #                     retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} INVIAVEL POR NUVEM E DENTRO DE NUVEM GENERICA'
            #                 else:
            #                     retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} INVIAVEL POR NUVEM E DENTRO DE NUVEM GENERICA'
            #     else:
            #         if v.VELOCIDADE <= 200:
            #             retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = 500
            #             retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = 159.99
            #             if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
            #                 retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
            #             else:
            #                 retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
            #         elif v.VELOCIDADE <= 600:
            #             retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = 500
            #             retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = 229.99
            #             if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
            #                 retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
            #             else:
            #                 retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
            #         else:
            #             if len(custo_medio[custo_medio.CNL == v.CNL]) > 0:
            #                 provedor_cm = custo_medio[custo_medio.CNL == v.CNL][f'PROVEDOR_{int(v.VELOCIDADE)}M'].values[0]
            #                 instalacao_cm = custo_medio[custo_medio.CNL == v.CNL][f'INST_{int(v.VELOCIDADE)}M'].values[0]
            #                 mensalidade_cm = custo_medio[custo_medio.CNL == v.CNL][f'MENSAL_{int(v.VELOCIDADE)}M'].values[0]
            #                 retorno_mapinfo.at[i,'PROVEDOR_CM'] = provedor_cm
            #                 retorno_mapinfo.at[i,'INSTALACAO_PROVEDOR_CM'] = instalacao_cm
            #                 retorno_mapinfo.at[i,'MENSALIDADE_PROVEDOR_CM'] = mensalidade_cm
            #                 if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
            #                     retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} SEM PRECO PARA VELOCIDADE SOLICITADA E DENTRO DE NUVEM GENERICA'
            #                 else:
            #                     retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} SEM PRECO PARA VELOCIDADE SOLICITADA E DENTRO DE NUVEM GENERICA'
            
            else:

                if v[p] == 'NOK':
                    if v['NUVEM_GENERICA'] == 'NOK':
                        
                        if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} INVIAVEL POR NUVEM E FORA DE NUVEM GENERICA'
                        else:
                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} INVIAVEL POR NUVEM E FORA DE NUVEM GENERICA'
                    else:
                        if len(custo_medio[custo_medio.CNL == v.CNL]) > 0:
                            
                            instalacao_cm = custo_medio[custo_medio.CNL == v.CNL][f'TERCEIROS INTERNET ASSIMÉTRICA INST - ATÉ {int(v.VELOCIDADE)}M'].values[0]
                            mensalidade_cm = custo_medio[custo_medio.CNL == v.CNL][f'TERCEIROS INTERNET ASSIMÉTRICA MENSAL - ATÉ {int(v.VELOCIDADE)}M'].values[0]
                            retorno_mapinfo.at[i,'PROVEDOR_CM'] = 'GENERICO - BANDA LARGA'
                            retorno_mapinfo.at[i,'INSTALACAO_PROVEDOR_CM'] = instalacao_cm
                            retorno_mapinfo.at[i,'MENSALIDADE_PROVEDOR_CM'] = mensalidade_cm
                            if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} INVIAVEL POR NUVEM E DENTRO DE NUVEM GENERICA'
                            else:
                                retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} INVIAVEL POR NUVEM E DENTRO DE NUVEM GENERICA'
                else:
                    
                    if v['TALKD'] != 'NOK':
                        
                        if retorno_mapinfo.at[i,f'STATUS_SHOPPING'] == 'INVIAVEL RESTRICAO SHOPPING':
                            valor = valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.OBS.str.contains('TABELA 2'))]
                            if len(valor) > 0:

                                # contrato = provedores[(provedores.PROVEDOR == p) & (provedores.SIGLA_MUNICIPIO == v.CNL)].NUM_CONTRATO.values[0]

                                # valor = valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.OBS.str.contains('TABELA 2'))]
                                if len(valor[(valor.VEL == v.VELOCIDADE) & (valor.PRAZO == f'{MESES} MESES')]) > 0:
                                    valor = valor[(valor.VEL == v.VELOCIDADE) & (valor.PRAZO == f'{MESES} MESES')]
                                    retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.TAXA_INSTALACAO.values[0]
                                    retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.CUSTO_MENSAL.values[0]
                                    if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = 'SHOPPING VIAVEL POR NUVEM CUSTO PADRAO'
                                    else:
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM'] = 'SHOPPING VIAVEL POR NUVEM CUSTO PADRAO'
                                elif len(valor[(valor.VEL == v.VELOCIDADE) & (valor.PRAZO == '12 MESES')]) > 0:
                                    valor = valor[(valor.VEL == v.VELOCIDADE) & (valor.PRAZO == '12 MESES')]
                                    retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.TAXA_INSTALACAO.values[0]
                                    retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.CUSTO_MENSAL.values[0]
                                    if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = 'SHOPPING VIAVEL POR NUVEM CUSTO PADRAO'
                                    else:
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM'] = 'SHOPPING VIAVEL POR NUVEM CUSTO PADRAO'
                                # valores[(valores.contrato == contrato) & valores.obs.str.contains('COMPARTILHADO') & (valores.VEL > v.VELOCIDADE) & (valores.tempo_contrato == '12 meses')]
                                elif len(valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.VEL > v.VELOCIDADE) & (valores.PRAZO == f'{MESES} MESES')]) > 0:
                                    
                                    valor = valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.OBS.str.contains('TABELA 2')) & (valores.VEL > v.VELOCIDADE) & (valores.PRAZO == f'{MESES} MESES')].sort_values(by=['VEL'],ascending=True)
                                    if len(valor) > 0:

                                        retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.TAXA_INSTALACAO.values[0]
                                        retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.CUSTO_MENSAL.values[0]
                                        if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = 'SHOPPING VIAVEL POR NUVEM CUSTO PADRAO'
                                        else:
                                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM'] = 'SHOPPING VIAVEL POR NUVEM CUSTO PADRAO'
                                    else:
                                        retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = 0
                                        retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = 0
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = 'SHOPPING VIAVEL POR NUVEM CUSTO PADRAO'
                                elif len(valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.VEL > v.VELOCIDADE) & (valores.PRAZO == '12 MESES')]) > 0:
                                    
                                    valor = valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.OBS.str.contains('TABELA 2')) & (valores.VEL > v.VELOCIDADE) & (valores.PRAZO == '12 MESES')].sort_values(by=['VEL'],ascending=True)
                                    if len(valor) > 0:

                                        retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.TAXA_INSTALACAO.values[0]
                                        retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.CUSTO_MENSAL.values[0]
                                        if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = 'SHOPPING VIAVEL POR NUVEM CUSTO PADRAO'
                                        else:
                                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM'] = 'SHOPPING VIAVEL POR NUVEM CUSTO PADRAO'
                                    else:
                                        retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = 0
                                        retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = 0
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = 'SHOPPING VIAVEL POR NUVEM CUSTO PADRAO'
                        else:
                            valor = valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.OBS.str.contains('TABELA 1'))]
                            if len(valor) > 0:

                                # contrato = provedores[(provedores.PROVEDOR == p) & (provedores.SIGLA_MUNICIPIO == v.CNL)].NUM_CONTRATO.values[0]

                                # valor = valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.OBS.str.contains('TABELA 1'))]
                                if len(valor[(valor.VEL == v.VELOCIDADE) & (valor.PRAZO == f'{MESES} MESES')]) > 0:
                                    valor = valor[(valor.VEL == v.VELOCIDADE) & (valor.PRAZO == f'{MESES} MESES')]
                                    retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.TAXA_INSTALACAO.values[0]
                                    retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.CUSTO_MENSAL.values[0]
                                    if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
                                    else:
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
                                # valores[(valores.contrato == contrato) & valores.obs.str.contains('COMPARTILHADO') & (valores.VEL > v.VELOCIDADE) & (valores.tempo_contrato == '12 meses')]
                                elif len(valor[(valor.VEL == v.VELOCIDADE) & (valor.PRAZO == '12 MESES')]) > 0:
                                    valor = valor[(valor.VEL == v.VELOCIDADE) & (valor.PRAZO == '12 MESES')]
                                    retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.TAXA_INSTALACAO.values[0]
                                    retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.CUSTO_MENSAL.values[0]
                                    if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
                                    else:
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
                                # valores[(valores.contrato == contrato) & valores.obs.str.contains('COMPARTILHADO') & (valores.VEL > v.VELOCIDADE) & (valores.tempo_contrato == '12 meses')]
                                elif len(valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.VEL > v.VELOCIDADE) & (valores.PRAZO == '12 MESES')]) > 0:
                                    
                                    valor = valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.OBS.str.contains('TABELA 1')) & (valores.VEL > v.VELOCIDADE) & (valores.PRAZO == '12 MESES')].sort_values(by=['VEL'],ascending=True)
                                    if len(valor) > 0:

                                        retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.TAXA_INSTALACAO.values[0]
                                        retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.CUSTO_MENSAL.values[0]
                                        if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA/VELOCIDADE CONSIDERADA {int(valor.VEL.values[0])}'
                                        else:
                                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA/VELOCIDADE CONSIDERADA {int(valor.VEL.values[0])}'
                                    else:
                                        retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = 0
                                        retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = 0
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'INVIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA'
                                elif len(valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.VEL > v.VELOCIDADE) & (valores.PRAZO == f'{MESES} MESES')]) > 0:
                                    
                                    valor = valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.OBS.str.contains('TABELA 1')) & (valores.VEL > v.VELOCIDADE) & (valores.PRAZO == f'{MESES} MESES')].sort_values(by=['VEL'],ascending=True)
                                    if len(valor) > 0:

                                        retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.TAXA_INSTALACAO.values[0]
                                        retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.CUSTO_MENSAL.values[0]
                                        if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA/VELOCIDADE CONSIDERADA {int(valor.VEL.values[0])}'
                                        else:
                                            retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA/VELOCIDADE CONSIDERADA {int(valor.VEL.values[0])}'
                                    else:
                                        retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = 0
                                        retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = 0
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'INVIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA'
                    else:

                        valor = valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL)]
                        if len(valor) > 0:

                            # contrato = provedores[(provedores.PROVEDOR == p) & (provedores.SIGLA_MUNICIPIO == v.CNL)].NUM_CONTRATO.values[0]
                            
                            valor = valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL)]
                            if len(valor[(valor.VEL == v.VELOCIDADE) & (valor.PRAZO == f'{MESES} MESES')]) > 0:
                                
                                valor = valor[(valor.VEL == v.VELOCIDADE) & (valor.PRAZO == f'{MESES} MESES')]
                                retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.TAXA_INSTALACAO.values[0]
                                retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.CUSTO_MENSAL.values[0]
                                if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                    retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
                                else:
                                    retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
                            # valores[(valores.contrato == contrato) & valores.obs.str.contains('COMPARTILHADO') & (valores.VEL > v.VELOCIDADE) & (valores.tempo_contrato == '12 meses')]
                            elif len(valor[(valor.VEL == v.VELOCIDADE) & (valor.PRAZO == '12 MESES')]) > 0:
                                
                                valor = valor[(valor.VEL == v.VELOCIDADE) & (valor.PRAZO == '12 MESES')]
                                retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.TAXA_INSTALACAO.values[0]
                                retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.CUSTO_MENSAL.values[0]
                                if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                    retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
                                else:
                                    retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/COM CONTRATO COM VELOCIDADE SOLICITADA'
                            # valores[(valores.contrato == contrato) & valores.obs.str.contains('COMPARTILHADO') & (valores.VEL > v.VELOCIDADE) & (valores.tempo_contrato == '12 meses')]
                            elif len(valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.VEL > v.VELOCIDADE) & (valores.PRAZO == f'{MESES} MESES')]) > 0:
                                
                                valor = valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.VEL > v.VELOCIDADE) & (valores.PRAZO == f'{MESES} MESES')].sort_values(by=['VEL'],ascending=True)
                                if len(valor) > 0:

                                    retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.TAXA_INSTALACAO.values[0]
                                    retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.CUSTO_MENSAL.values[0]
                                    if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA/VELOCIDADE CONSIDERADA {int(valor.VEL.values[0])}'
                                    else:
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA/VELOCIDADE CONSIDERADA {int(valor.VEL.values[0])}'
                                else:
                                    retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = 0
                                    retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = 0
                                    retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'INVIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA'
                            elif len(valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.VEL > v.VELOCIDADE) & (valores.PRAZO == '12 MESES')]) > 0:
                                
                                valor = valores[(valores.PROVEDOR == p) & (valores.SIGLA_MUNICIPIO == v.CNL) & (valores.VEL > v.VELOCIDADE) & (valores.PRAZO == '12 MESES')].sort_values(by=['VEL'],ascending=True)
                                if len(valor) > 0:

                                    retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = valor.TAXA_INSTALACAO.values[0]
                                    retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = valor.CUSTO_MENSAL.values[0]
                                    if retorno_mapinfo.at[i,f'STATUS_NUVEM'] == '':
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'{p} VIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA/VELOCIDADE CONSIDERADA {int(valor.VEL.values[0])}'
                                    else:
                                        retorno_mapinfo.at[i,f'STATUS_NUVEM'] = retorno_mapinfo.at[i,f'STATUS_NUVEM']+f'/{p} VIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA/VELOCIDADE CONSIDERADA {int(valor.VEL.values[0])}'
                                else:
                                    retorno_mapinfo.at[i,f'INSTALACAO_PROVEDOR_{p}'] = 0
                                    retorno_mapinfo.at[i,f'MENSALIDADE_PROVEDOR_{p}'] = 0
                                    retorno_mapinfo.at[i,f'STATUS_NUVEM'] = f'INVIAVEL POR NUVEM/SEM CONTRATO COM VELOCIDADE SOLICITADA'

    for i,v in retorno_mapinfo.iterrows():
        melhor_instalacao = 0
        melhor_mensalidade = 0
        instalacao_provedor_cm = 0
        mensalidade_provedor_cm = 0
        status_nuvem_cm = ''
        melhor_provedor = ''

        if v.STATUS_SHOPPING == 'INVIAVEL RESTRICAO SHOPPING':
            if v.TALKD == 'NOK':
                inviaveis.at[len(inviaveis),'SEV'] = v.SEV
                inviaveis.at[len(inviaveis)-1,'MOTIVO'] = v.STATUS_SHOPPING
                inviaveis.at[len(inviaveis)-1,'FASE'] = v.FASE
                inviaveis.at[len(inviaveis)-1,'ID_ANALISE'] = v.ID_SEV
                inviaveis.at[len(inviaveis)-1,'UF'] = v.UF
                try:

                    inviaveis.at[len(inviaveis)-1,'LATITUDE'] = v.LATITUDE
                    inviaveis.at[len(inviaveis)-1,'LONGITUDE'] = v.LONGITUDE
                except:
                    inviaveis.at[len(inviaveis)-1,'LATITUDE'] = v.LAT
                    inviaveis.at[len(inviaveis)-1,'LONGITUDE'] = v.LONG

                retorno_mapinfo.at[i,'INVIAVEL'] = 'S'
                continue
        
        for p in provedores_nuvem.PROVEDOR.values.tolist():
            if v[f'MENSALIDADE_PROVEDOR_{p}'] == 0:
                if melhor_mensalidade == 0:
                    if v['MENSALIDADE_PROVEDOR_CM'] == 0:
                        if v['PROVEDOR_CM'] == '-':
                            inviaveis.at[len(inviaveis),'SEV'] = v.SEV
                            inviaveis.at[len(inviaveis)-1,'MOTIVO'] = 'INVIAVEL POR NUVENS TERCEIROS/SEM PROVEDOR COM CUSTO MEDIO NO LOCAL'
                            inviaveis.at[len(inviaveis)-1,'FASE'] = v.FASE
                            inviaveis.at[len(inviaveis)-1,'ID_ANALISE'] = v.ID_SEV
                            inviaveis.at[len(inviaveis)-1,'UF'] = v.UF
                            try:

                                inviaveis.at[len(inviaveis)-1,'LATITUDE'] = v.LATITUDE
                                inviaveis.at[len(inviaveis)-1,'LONGITUDE'] = v.LONGITUDE
                            except:
                                inviaveis.at[len(inviaveis)-1,'LATITUDE'] = v.LAT
                                inviaveis.at[len(inviaveis)-1,'LONGITUDE'] = v.LONG

                            retorno_mapinfo.at[i,'INVIAVEL'] = 'S'
                        else:
                            inviaveis.at[len(inviaveis),'SEV'] = v.SEV
                            inviaveis.at[len(inviaveis)-1,'MOTIVO'] = v.STATUS_NUVEM
                            inviaveis.at[len(inviaveis)-1,'FASE'] = v.FASE
                            inviaveis.at[len(inviaveis)-1,'ID_ANALISE'] = v.ID_SEV
                            inviaveis.at[len(inviaveis)-1,'UF'] = v.UF
                            try:

                                inviaveis.at[len(inviaveis)-1,'LATITUDE'] = v.LATITUDE
                                inviaveis.at[len(inviaveis)-1,'LONGITUDE'] = v.LONGITUDE
                            except:
                                inviaveis.at[len(inviaveis)-1,'LATITUDE'] = v.LAT
                                inviaveis.at[len(inviaveis)-1,'LONGITUDE'] = v.LONG

                            retorno_mapinfo.at[i,'INVIAVEL'] = 'S'
                    else:
                        provedor_cm = v.PROVEDOR_CM
                        instalacao_provedor_cm = v.INSTALACAO_PROVEDOR_CM
                        mensalidade_provedor_cm = v.MENSALIDADE_PROVEDOR_CM
                        status_nuvem_cm = v.STATUS_NUVEM
            else:
                if melhor_provedor == '':
                    
                    melhor_provedor = p
                    melhor_instalacao = v[f'INSTALACAO_PROVEDOR_{p}']
                    melhor_mensalidade = v[f'MENSALIDADE_PROVEDOR_{p}']
                    retorno_mapinfo.at[i,'INVIAVEL'] = ''
                else:
                    if (melhor_mensalidade + (melhor_instalacao / MESES)) > (v[f'MENSALIDADE_PROVEDOR_{p}'] + (v[f'INSTALACAO_PROVEDOR_{p}'] / MESES)):
                        melhor_provedor = p
                        melhor_instalacao = v[f'INSTALACAO_PROVEDOR_{p}']
                        melhor_mensalidade = v[f'MENSALIDADE_PROVEDOR_{p}']
                        retorno_mapinfo.at[i,'INVIAVEL'] = ''

        if melhor_mensalidade != 0:
            retorno_mapinfo.at[i,'ESCOLHA_PROVEDOR_FINAL'] = melhor_provedor
            retorno_mapinfo.at[i,'INSTALACAO_PROVEDOR_FINAL'] = melhor_instalacao
            retorno_mapinfo.at[i,'MENSALIDADE_PROVEDOR_FINAL'] = melhor_mensalidade
            retorno_mapinfo.at[i,'MOTIVO_ESCOLHA'] = v.STATUS_NUVEM
        else:
            try:
                retorno_mapinfo.at[i,'ESCOLHA_PROVEDOR_FINAL'] = provedor_cm
                retorno_mapinfo.at[i,'INSTALACAO_PROVEDOR_FINAL'] = instalacao_provedor_cm
                retorno_mapinfo.at[i,'MENSALIDADE_PROVEDOR_FINAL'] = mensalidade_provedor_cm
                retorno_mapinfo.at[i,'MOTIVO_ESCOLHA'] = status_nuvem_cm
            except:
                pass


    try:
        retorno_mapinfo = retorno_mapinfo[retorno_mapinfo.INVIAVEL != 'S'].drop(columns=['INVIAVEL'])
    except:
        pass


    for i, v in retorno_mapinfo.iterrows():
        retorno_mapinfo.at[i,'INSTALACAO_PROVEDOR_FINAL'] = str(round(v.INSTALACAO_PROVEDOR_FINAL,2)).replace('.',',')
        retorno_mapinfo.at[i,'MENSALIDADE_PROVEDOR_FINAL'] = str(round(v.MENSALIDADE_PROVEDOR_FINAL,2)).replace('.',',')

    inviaveis['REMOVER'] = ''

    for i,v in inviaveis.iterrows():
        if len(retorno_mapinfo[retorno_mapinfo.SEV == v.SEV]) > 0:
            inviaveis.at[i,'REMOVER'] = 'X'
            
    inviaveis = inviaveis[inviaveis.REMOVER != 'X'].drop(columns=["REMOVER"])
    inviaveis = inviaveis.drop_duplicates().reset_index(drop=True)


    for i, v in retorno_mapinfo.iterrows():
        provedor = ids_provedores[(ids_provedores.PROVEDOR == v.ESCOLHA_PROVEDOR_FINAL) & (ids_provedores.UF == v.UF)]
        if v.ESCOLHA_PROVEDOR_FINAL == 'GENERICO - BANDA LARGA':
            retorno_mapinfo.at[i,'EMPRESA'] = 'GENERICO - BANDA LARGA'
            retorno_mapinfo.at[i,'ID_TEIA'] = 1826
        elif len(provedor) <= 0:
            retorno_mapinfo.at[i,'INVIAVEL'] = 'S'
        else:
            retorno_mapinfo.at[i,'EMPRESA'] = provedor.EMPRESA.values[0]
            retorno_mapinfo.at[i,'ID_TEIA'] = provedor['ID TEIA'].values[0]


    for i,v in retorno_mapinfo.iterrows():
        try:
            if v.INVIAVEL == 'S':
                inviaveis.at[len(inviaveis),'SEV'] = v.SEV
                inviaveis.at[len(inviaveis)-1,'MOTIVO'] = f'PROVEDOR {v.ESCOLHA_PROVEDOR_FINAL} NAO CADASTRADO NO SISTEMA'
                inviaveis.at[len(inviaveis)-1,'SEV'] = v.SEV
                inviaveis.at[len(inviaveis)-1,'FASE'] = v.FASE
                inviaveis.at[len(inviaveis)-1,'ID_ANALISE'] = v.ID_SEV
                inviaveis.at[len(inviaveis)-1,'UF'] = v.UF
                try:

                    inviaveis.at[len(inviaveis)-1,'LATITUDE'] = v.LATITUDE
                    inviaveis.at[len(inviaveis)-1,'LONGITUDE'] = v.LONGITUDE
                except:
                    inviaveis.at[len(inviaveis)-1,'LATITUDE'] = v.LAT
                    inviaveis.at[len(inviaveis)-1,'LONGITUDE'] = v.LONG
            continue
        except:
            pass
    try:
        retorno_mapinfo = retorno_mapinfo[retorno_mapinfo.INVIAVEL != 'S'].drop(columns=['INVIAVEL'])
    except:
        pass


    for i,v in retorno_mapinfo.iterrows():
        if v.STATUS_SHOPPING == 'INVIAVEL RESTRICAO SHOPPING':
            retorno_mapinfo.at[i,'obs'] = 'SHOPPING VIAVEL POR NUVEM '

        if v.ESCOLHA_PROVEDOR_FINAL in provedores_nuvem.PROVEDOR.values.tolist():
            try:
                retorno_mapinfo.at[i,'obs'] += 'LPU PADRAO - NUVEM PROVEDOR'
            except:
                retorno_mapinfo.at[i,'obs'] = 'LPU PADRAO - NUVEM PROVEDOR'

        elif v.ESCOLHA_PROVEDOR_FINAL not in provedores_nuvem.PROVEDOR.values.tolist():
            try:
                retorno_mapinfo.at[i,'obs'] += 'LPU PADRAO - NUVEM GENERICA'
            except:
                retorno_mapinfo.at[i,'obs'] = 'LPU PADRAO - NUVEM GENERICA'

        

        if v.FASE == 'Consulta Provedor Terceiro':
            try:
                retorno_mapinfo.at[i,'obs'] += '/ENTREGA COM IP DINAMICO, PARA IP FIXO SOLICITAR VIA DEMANDA PROJETIZADA [POS ANALISE DE HP]'
            except:
                retorno_mapinfo.at[i,'obs'] = '/ENTREGA COM IP DINAMICO, PARA IP FIXO SOLICITAR VIA DEMANDA PROJETIZADA [POS ANALISE DE HP]'
        else:

            try:
                retorno_mapinfo.at[i,'obs'] += '/ENTREGA COM IP DINAMICO, PARA IP FIXO SOLICITAR VIA DEMANDA PROJETIZADA'
            except:
                retorno_mapinfo.at[i,'obs'] = '/ENTREGA COM IP DINAMICO, PARA IP FIXO SOLICITAR VIA DEMANDA PROJETIZADA'




    fechamento = pd.DataFrame()

    fechamento[['sequencial','latitude','longitude','uf','cnl','provedor','id_provedor','instalacao_terceiros',
            'mensalidade_terceiros','id_da_sev','obs']] = retorno_mapinfo[['SEV','LAT','LONG','UF','CNL','EMPRESA','ID_TEIA','INSTALACAO_PROVEDOR_FINAL','MENSALIDADE_PROVEDOR_FINAL','ID_SEV','obs']]

    fechamento['facilidade'] = 'TERCEIROS ETH'
    fechamento['id_facilidade'] = 26
    fechamento['abordado'] = 'NAO'
    fechamento['tipo_terceiros'] = 3
    fechamento['status'] = 1
    fechamento['ID Justificativa'] = 1
    fechamento['justificativa'] = 'FORA DE REDE'
    fechamento['protocolo_gaia'] = 0
    fechamento[['prazo','bb_ip','hp_bsod','codigo_spe','sinalizacao_sip','custo_de_acesso_proprio']] = ''

    for i,v in fechamento.iterrows():
        estacao = estacoes[estacoes.UF == v.uf].ESTACAO.values[0]
        fechamento.at[i,'entrega'] = estacao
        fechamento.at[i,'id_provedor'] = str(v.id_provedor).split('.')[0]
        fechamento.at[i,'id_da_sev'] = str(v.id_da_sev).split('.')[0]
        fechamento.at[i,'instalacao_terceiros'] = str(v.instalacao_terceiros).replace('.',',')
        fechamento.at[i,'mensalidade_terceiros'] = str(v.mensalidade_terceiros).replace('.',',')

    fechamento = fechamento[['sequencial','latitude','longitude','uf','cnl','facilidade','id_facilidade','provedor','id_provedor',
    'entrega','abordado','custo_de_acesso_proprio','instalacao_terceiros','mensalidade_terceiros',
    'tipo_terceiros','id_da_sev','prazo','bb_ip','hp_bsod','codigo_spe','sinalizacao_sip',
    'protocolo_gaia','obs','justificativa','ID Justificativa','status'
    ]]

    for i, v in fechamento.iterrows():
        if isinstance( v.latitude, float ):
            fechamento.at[i,'latitude'] = str(v.latitude).replace('.',',')
        if isinstance( v.longitude, float ):
            fechamento.at[i,'longitude'] = str(v.longitude).replace('.',',')

    inviaveis = inviaveis.drop_duplicates()
    fechamento = fechamento.drop_duplicates()


    button_mapinfo = ctk.CTkButton(janela,text="Arquivo Mapinfo",height=20,width=35,corner_radius=8,fg_color='green',hover_color='blue', command=arquivo_retorno)
    button_mapinfo.place(x=10,y=160)

    button_gera_mapinfo = ctk.CTkButton(janela,text="Gerar Arquivo para fechamento",height=20,width=35,corner_radius=8,fg_color='grey',hover_color='blue', command=gerar_fechamento)
    button_gera_mapinfo.place(x=250,y=200)


def gerar_fechamento():
    
    inviaveis.to_excel(f'SAIDAS/sevs_inviaveis_{now}.xlsx',index=False)

    fechamento.to_csv(f'SAIDAS/fechamento_teia_{now}.csv',sep=';',index=False)

    button_gera_mapinfo = ctk.CTkButton(janela,text="Gerar Arquivo para fechamento",height=20,width=35,corner_radius=8,fg_color='green',hover_color='blue', command=gerar_fechamento)
    button_gera_mapinfo.place(x=250,y=200)

janela.title("BLC TERCEIROS")
janela.geometry("500x400")
janela.resizable(width=False, height=False)

titulo_teia= ctk.CTkLabel(janela,text='Selecione o arquivo de entrada (TEIA)')
titulo_teia.place(x=10,y=70)

button_browse = ctk.CTkButton(janela,text="Arquivo TEIA",height=20,width=35,corner_radius=8,fg_color='grey',hover_color='blue', command=arquivo_entrada)
button_browse.place(x=10,y=100)

titulo_meses= ctk.CTkLabel(janela,text='Selecione a quantidade de meses')
titulo_meses.place(x=250,y=70)

meses = ctk.CTkOptionMenu(janela,values=['12','24','36','48','60','72','84'],fg_color='grey',button_color='grey',button_hover_color='blue')
meses.place(x=250,y=100)

button_gera_mapinfo = ctk.CTkButton(janela,text="Gerar Arquivo para mapinfo",height=20,width=35,corner_radius=8,fg_color='grey',hover_color='blue', command=gerar_arq_mapinfo)
button_gera_mapinfo.place(x=10,y=200)

janela.mainloop()