import pandas as pd
import re
import os
import numpy as np

def corrige_txt(nom_estacao, ano, mes):
    for num1 in ano:
        for num2 in mes:
            file = "D:/Documentos/GitHub/estacoes_pluviometricas/Dados/DadosPluviometricos/" + nom_estacao + "_" + num1 + num2 + "_Plv.txt"
            if os.path.exists(file):
                fin = open(file, "rt")
                if not os.path.exists("D:/Documentos/GitHub/estacoes_pluviometricas/Dados/DadosPluviometricos/aux_"+ nom_estacao):
                    os.mkdir("D:/Documentos/GitHub/estacoes_pluviometricas/Dados/DadosPluviometricos/aux_"+ nom_estacao)
                fout = open("D:/Documentos/GitHub/estacoes_pluviometricas/Dados/DadosPluviometricos/aux_" + nom_estacao + "/" + nom_estacao + "_" + num1 + num2 + "_Met2.txt", "wt")
                count = 0
                for line in fin:
                    count += 1
                    if nom_estacao == 'guaratiba':
                        fout.write(re.sub('\s+', ' ', line.replace(':40      ', ':00  HBV')))
                    else:
                        fout.write(re.sub('\s+', ' ', line.replace(':00      ', ':00  HBV')))
                    fout.write('\n')
                
                fout.write('01/' + num2 + '/'+num1 + ' 00:00:00 HBV ND ND ND ND ND')
                fin.close()
                fout.close()
            else:
                pass


def gera_dataset(nom_estacao, ano, mes):
    data1 = pd.DataFrame()

    for num1 in ano:
        check = 0
        for num2 in mes:
            texto = 'D:/Documentos/GitHub/estacoes_pluviometricas/Dados/DadosPluviometricos/aux_' + nom_estacao + '/' + nom_estacao + '_' + num1 + num2 + '_Met2.txt'
            if os.path.exists(texto):
                data1 = pd.read_csv(texto, sep=' ', skiprows=[0, 1, 2, 3, 4], header=None)
                if len(data1.columns) == 9:
                    data1.columns = ['Dia', 'Hora', 'HBV', '15 min', '01 h','04 h', '24 h', '96 h', 'teste']
                    del data1["teste"]
                else:
                    data1.columns = ['Dia', 'Hora', 'HBV', '15 min', '01 h','04 h', '24 h', '96 h']
                data1['15 min'] = data1['15 min'][~data1['15 min'].isin(['-', 'ND'])].astype(float)
                data1['01 h'] = data1['01 h'][~data1['01 h'].isin(['-', 'ND'])].astype(float)
                data1['04 h'] = data1['04 h'][~data1['04 h'].isin(['-', 'ND'])].astype(float)
                data1['24 h'] = data1['24 h'][~data1['24 h'].isin(['-', 'ND'])].astype(float)
                data1['96 h'] = data1['96 h'][~data1['96 h'].isin(['-', 'ND'])].astype(float)
                data1['Dia'] = pd.to_datetime(data1['Dia'], format='%d/%m/%Y')
                ano_aux = num1
                mes_aux = num2
                print(num1 + '/' + num2)
                check = 1
                break
            else:
                pass
        if check == 1:
            break
        
    ano1 = list(map(str,range(int(ano_aux),2022)))
    mes1 = list(range(int(mes_aux),13))
    mes1 = [str(i).rjust(2, '0') for i in mes1]

    for num1 in ano1:
        for num2 in mes1:
            texto = 'D:/Documentos/GitHub/estacoes_pluviometricas/Dados/DadosPluviometricos/aux_' + nom_estacao + '/' + nom_estacao + '_' + num1 + num2 + '_Met2.txt'
            if os.path.exists(texto):
                data2 = pd.read_csv(texto, sep=' ', skiprows=[0, 1, 2, 3, 4], header=None, on_bad_lines='skip')
                if len(data2.columns) == 9:
                    data2.columns = ['Dia', 'Hora', 'HBV', '15 min', '01 h','04 h', '24 h', '96 h', 'teste']
                    del data2["teste"]
                else:
                    data2.columns = ['Dia', 'Hora', 'HBV', '15 min', '01 h','04 h', '24 h', '96 h']
                data2['15 min'] = data2['15 min'][~data2['15 min'].isin(['-', 'ND'])].astype(float)
                data2['01 h'] = data2['01 h'][~data2['01 h'].isin(['-', 'ND'])].astype(float)
                data2['04 h'] = data2['04 h'][~data2['04 h'].isin(['-', 'ND'])].astype(float)
                data2['24 h'] = data2['24 h'][~data2['24 h'].isin(['-', 'ND'])].astype(float)
                data2['96 h'] = data2['96 h'][~data2['96 h'].isin(['-', 'ND'])].astype(float)
                data2['Dia'] = pd.to_datetime(data2['Dia'], format='%d/%m/%Y')
                print(num1 + '/' + num2)

                saida = pd.concat([data1, data2])
                data1 = saida
                del saida
            else:
                pass
    if num1 == ano_aux:
        mes1 = list(range(1,13))
        mes1 = [str(i).rjust(2, '0') for i in mes1]
    data1 = data1.replace('ND', np.NaN)
    data1 = data1.replace('-', np.NaN)
    data1['estacao'] = nom_estacao
    data1.to_csv(nom_estacao + '.csv')
    data_aux = data1
    del data1, data2
    return data_aux


def pre_processamento(nom_estacao):
    ano  = list(map(str,range(1997,2022)))
    mes = list(range(1,13))
    mes = [str(i).rjust(2, '0') for i in mes]

    corrige_txt(nom_estacao, ano, mes)

    data = gera_dataset(nom_estacao, ano, mes)
    #data.plot.line('Dia', ['Chuva'], title='Chuva')
    #data.plot.line('Dia', ['Temperatura'], title='Temperatura')
    #data.plot.line('Dia', ['Umidade'], title='Umidade')
    del data

estacoes = ['alto_da_boa_vista','anchieta','av_brasil_mendanha','bangu','campo_grande','cidade_de_deus','copacabana','grajau','grajau_jacarepagua','grande_meier','grota_funda','guaratiba','ilha_do_governador','iraja','jardim_botanico','laranjeiras','madureira','penha','piedade','recreio','riocentro','rocinha','santa_cruz','santa_teresa','sao_cristovao','saude','sepetiba','tanque','tijuca','tijuca_muda','urca','vidigal']

for i in estacoes:
    print(i)
    pre_processamento(i)