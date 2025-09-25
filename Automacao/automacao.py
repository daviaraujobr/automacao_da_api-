import requests
import pandas as pd
import re
import difflib
import locale

# Dicionário com siglas dos HU
MAPA_HUS_KEYWORDS = {
    "Hospital de Clínicas da Universidade Federal do Paraná": "CHC-UFPR",
    "Hospital Universitário da Universidade Federal do Maranhão": "HU-UFMA",
    "Hospital Universitário Lauro Wanderley": "HULW-UFPB",
    "Hospital Universitário Onofre Lopes": "HUOL-UFRN",
    "Maternidade Escola Januário Cicco": "MEJC-UFRN",
    "Hospital Universitário Antônio Pedro": "HUAP-UFF",
    "Hospital Universitário da Universidade Federal do Piauí": "HU-UFPI",
    "Hospital Universitário da Universidade Federal de Sergipe": "HU-UFS",
    "Hospital Universitário do Vale do São Francisco": "HU-UNIVASF",
    "Hospital de Ensino Dr. Washington Antônio de Barros da Universidade Federal do Vale do São Francisco": "HU-UNIVASF",
    "Hospital Universitário da Universidade Federal de Santa Catarina": "HU-UFSC",
    "Hospital Universitário de Brasília": "HUB-UNB",
    "Hospital Universitário da Universidade Federal de Juiz de Fora": "HU-UFJF",
    "Hospital Universitário da Universidade Federal de Goiás": "HU-UFG",
    "Hospital das Clínicas da Universidade Federal de Goiás": "HC-UFG",
    "Hospital Universitário da Universidade Federal de São Carlos": "HU-UFSCar",
    "Hospital de Clínicas da Universidade Federal de Uberlândia": "HC-UFU",
    "Hospital Universitário Júlio Müller": "HUJM-UFMT",
    "Hospital Universitário da Universidade Federal de Mato Grosso": "HU-UFMT",
    "Hospital Universitário da Universidade Federal de Mato Grosso do Sul": "HU-UFMS",
    "Hospital Universitário da Universidade Federal de Pelotas": "HU-UFPel",
    "Hospital Universitário da Universidade Federal do Rio Grande": "HU-FURG",
    "Hospital Universitário de Santa Maria": "HUSM-UFSM",
    "Hospital Universitário da Universidade Federal do Rio Grande do Norte": "HU-UFRN",
    "Hospital Universitário da Universidade Federal de Campina Grande": "HU-UFCG",
    "Hospital Universitário Alcides Carneiro": "HUAC-UFCG",
    "Hospital Universitário Walter Cantídio": "HUWC-UFC",
    "Complexo Hospitalar da Universidade Federal do Ceará": "HUWC-UFC",
    "Hospital Universitário da Universidade Federal do Ceará": "HU-UFC",
    "Hospital Universitário Professor Edgard Santos": "HUPES-UFBA",
    "Maternidade Climério de Oliveira": "MCO-UFBA",
    "Hospital Universitário da Universidade Federal do Oeste da Bahia": "HU-UFOB",
    "Hospital Universitário da Universidade Federal do Amapá": "HU-UNIFAP",
    "Hospital Universitário João de Barros Barreto": "HUJBB-UFPA",
    "Hospital Universitário Bettina Ferro de Souza": "HUBFS-UFPA",
    "Hospital Universitário da Universidade Federal do Pará": "HU-UFPA",
    "Hospital Universitário Getúlio Vargas": "HUGV-UFAM",
    "Hospital Universitário da Universidade Federal de Rondônia": "HU-UNIR",
    "Hospital Universitário da Universidade Federal do Acre": "HU-UFAC",
    "Hospital Universitário da Universidade Federal do Tocantins": "HU-UFT",
    "Hospital Universitário da Universidade Federal de Roraima": "HU-UFRR",
    "Hospital Universitário da Universidade Federal do Espírito Santo": "HU-UFES"
}

class Requests:
    def __init__(self):
        self.chave_api = ""
        if not self.chave_api:
            raise ValueError("API key não informada!")

        self.base_url = (
            'https://eaud.cgu.gov.br/api/auth/monitoramento/beneficio?apenasAbertas=false'
            '&exibirColunaPendencias=false&apenasModificadasNosUltimos30Dias=false'
            '&colunaOrdenacao=id&direcaoOrdenacao=DESC'
            '&colunasSelecionadas=id%2Csituacao%2Cestado%2Catividade%2Ctitulo%2CidTarefaAssociada'
            '%2CatividadeTarefaAssociada%2CtituloTarefaAssociada%2CdtPrevisaoInicio%2CdtPrevisaoFim'
            '%2CdtRealizadaInicio%2CdtRealizadaFim%2Cprioridade%2Ctags%2CexportarPara%2CdimensaoME'
            '%2Crepercussao%2CunidadeProponente%2CunidadeGestora%2CanoFatoGerador%2CanoImplementacao'
            '%2CdescricaoCusto%2Cnivel%2CclasseBeneficio%2CtipoBeneficio%2CunidadesEnvolvidas'
            '%2CvalorCusto%2CvalorLiquido%2CvalorBruto%2CvalorBrutoSomatorio'
            '&mostrarTarefaBloqueadaPgd=true'
        )
                      
        self.headers = {'chave-api': self.chave_api}
        self.df = None
        
        self.colunas = [
            "unidadeProponente",
            "tipoBeneficio",
            "anoImplementacao",
            "id",
            "titulo",
            "tituloTarefaAssociada",
            "valorLiquido",
        ]
        
        self.colunas_formatadas = {
            "unidadeProponente": "UNIDADE_TEMP",
            "tipoBeneficio": "CLASSE DO BENEFÍCIO",
            "anoImplementacao": "Ano",
            "id": "ID",
            "titulo": "RECOMENDAÇÃO",
            "tituloTarefaAssociada": "PRODUTO",
            "valorLiquido": "VALOR (R$)",
        }

    def clear_text(self, text):
        if isinstance(text, str):
            return re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]', '', text)
        return text

    def extrair_sigla_por_nome(self, value):
        if not isinstance(value, str) or value.strip() == "":
            return ""
        
        for nome, sigla in MAPA_HUS_KEYWORDS.items():
            if nome.lower() in value.lower():
                return sigla

        match = re.search(r'\b([A-Z]{1,4}-[A-Z]{2,5})\b', value)
        if match:
            return match.group(1)

        nomes = list(MAPA_HUS_KEYWORDS.keys())
        match = difflib.get_close_matches(value, nomes, n=1, cutoff=0.6)
        if match:
            return MAPA_HUS_KEYWORDS[match[0]]

        return ""

    def get_data(self, page_size=100):
        print('Iniciando a coleta de dados da API...')
        all_items = []
        offset = 0

        while True:
            url = f"{self.base_url}&tamanhoPagina={page_size}&offset={offset}"
            response = requests.get(url, headers=self.headers, timeout=30)

            if response.status_code != 200:
                print(f"Erro HTTP {response.status_code} - {response.text}")
                break

            data = response.json()
            itens = data.get("data", [])
            print(f"Offset={offset} | recebidos={len(itens)}")

            if not itens:
                break

            all_items.extend(itens)
            if len(itens) < page_size:
                break
            offset += page_size

        if not all_items:
            print("Nenhum dado retornado")
            self.df = pd.DataFrame()
            return self.df

        df_temp = pd.json_normalize(all_items)
        
        self.df = df_temp[self.colunas].copy()
        self.df['Documento'] = '#' + self.df['id'].astype(str)
        self.df.rename(columns=self.colunas_formatadas, inplace=True)
        self.df["UNIDADE"] = self.df["UNIDADE_TEMP"].apply(self.extrair_sigla_por_nome)
        self.df["UNIDADE"] = self.df["UNIDADE"].apply(lambda x: x if x != "" else "EBSERH")
        self.df.drop(columns=["UNIDADE_TEMP"], inplace=True)
        self.df["CLASSE DO BENEFÍCIO"] = self.df["CLASSE DO BENEFÍCIO"].replace({
            "Qualitativo": "Não Financeiro",
            "Financeiro": "Financeiro"
        })
        self.df.insert(0, 'ITEM', range(1, 1 + len(self.df)))

        ordem_final = [
            'ITEM', 'UNIDADE', 'CLASSE DO BENEFÍCIO', 'Ano', 'ID', 'Documento',
            'RECOMENDAÇÃO', 'PRODUTO', 'VALOR (R$)'
        ]
        self.df = self.df[ordem_final]

        print(f"Total coletado: {len(self.df)}")
        return self.df

    def save_excel_with_total(self, nome_arquivo='Beneficio2025.xlsx'):
        if self.df is not None and not self.df.empty:
            df_clean = self.df.copy()
            for col in df_clean.columns:
                if df_clean[col].dtype == 'object':
                    df_clean[col] = df_clean[col].apply(self.clear_text)

            def limpar_valor(valor):
                if pd.isna(valor):
                    return 0
                if isinstance(valor, (int, float)):
                    return valor
                
                s = str(valor)
                s_limpo = re.sub(r'[^\d,-]', '', s)
                s_limpo = s_limpo.replace(',', '.')
                
                try:
                    return float(s_limpo)
                except (ValueError, TypeError):
                    return 0

            df_clean['VALOR (R$)'] = df_clean['VALOR (R$)'].apply(limpar_valor)
            total_valor = df_clean['VALOR (R$)'].sum()

            try:
                locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
                valor_formatado = locale.currency(total_valor, grouping=True)
            except locale.Error:
                valor_formatado = f"R$ {total_valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

            df_total = pd.DataFrame({
                'Total Benefício Financeiro': [valor_formatado]
            })
            
            print(f"Valor total calculado e formatado: {valor_formatado}")

            # Função para auto-ajustar a largura das colunas
            def auto_adjust_column_width(worksheet):
                for column_cells in worksheet.columns:
                    max_length = 0
                    column_letter = column_cells[0].column_letter
                    for cell in column_cells:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = (max_length + 2)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

            # Salvar Excel com duas abas e ajustar colunas
            with pd.ExcelWriter(nome_arquivo, engine='openpyxl') as writer:
                # Escreve a primeira aba
                df_clean.to_excel(writer, sheet_name='Beneficios', index=False)
                worksheet_beneficios = writer.sheets['Beneficios']
                
                # Aplica formato de moeda na coluna de valor (Coluna I)
                # Este formato mostra negativos em vermelho com parênteses
                currency_format = 'R$ #,##0.00;[Red]-R$ #,##0.00'
                for cell in worksheet_beneficios['I']:
                    if not cell.row == 1: # Pula o cabeçalho
                        cell.number_format = currency_format
                
                # Ajusta a largura das colunas na primeira aba
                auto_adjust_column_width(worksheet_beneficios)

                # Escreve a segunda aba
                df_total.to_excel(writer, sheet_name='Valor Total', index=False)
                worksheet_total = writer.sheets['Valor Total']
                
                # Ajusta a largura das colunas na segunda aba
                auto_adjust_column_width(worksheet_total)
            
            print(f"Dados salvos em '{nome_arquivo}'")
        else:
            print("Nenhum dado para salvar. Execute get_data() primeiro.")


if __name__ == "__main__":
    req = Requests()
    df = req.get_data(page_size=100)
    if df is not None and not df.empty:
        print("\nAmostra dos dados formatados:")
        print(df.head())
        print("-" * 50)
        req.save_excel_with_total()