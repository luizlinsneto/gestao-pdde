import streamlit as st
import pandas as pd
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
import json
import os

# Configuração da Página
st.set_page_config(page_title="Gestão Financeira Escolar - PDDE", layout="wide")

# --- ESTILOS CSS ---
st.markdown("""
    <style>
    .stNumberInput input { text-align: right; }
    .big-font { font-size: 18px !important; font-weight: bold; }
    div[data-testid="stMetricValue"] { font-size: 24px; }
    </style>
    """, unsafe_allow_html=True)

# --- CONEXÃO COM FIREBASE ---
@st.cache_resource
def init_firebase():
    """Inicializa a conexão com Firebase apenas uma vez e mantém em cache."""
    if not firebase_admin._apps:
        cred = None
        if os.path.exists("firebase_key.json"):
            try:
                cred = credentials.Certificate("firebase_key.json")
            except Exception as e:
                st.error(f"Erro no arquivo json: {e}")
                return None
        else:
            try:
                if hasattr(st, "secrets") and "firebase" in st.secrets:
                    cred_info = dict(st.secrets["firebase"])
                    cred = credentials.Certificate(cred_info)
            except Exception:
                pass
        
        if cred:
            firebase_admin.initialize_app(cred)
            return firestore.client()
        else:
            return None
    return firestore.client()

# --- FUNÇÕES DE BANCO DE DADOS (CRUD) ---
def load_accounts_from_firebase(db):
    if db is None: return {}
    try:
        accounts_ref = db.collection('pdde_contas')
        docs = accounts_ref.stream()
        dados = {}
        for doc in docs:
            dados[doc.id] = doc.to_dict()
        return dados
    except Exception as e:
        st.error(f"Erro ao ler contas: {e}")
        return {}

def load_empenhos_from_firebase(db):
    if db is None: return []
    try:
        doc = db.collection('pdde_dados_gerais').document('empenhos').get()
        if doc.exists:
            return doc.to_dict().get('lista', [])
        return []
    except Exception as e:
        return []

def load_global_programs_from_firebase(db):
    if db is None: return []
    try:
        doc = db.collection('pdde_dados_gerais').document('programas_globais').get()
        if doc.exists:
            return doc.to_dict().get('lista', [])
        return []
    except Exception as e:
        return []

def save_account_to_firebase(db, account_name, account_data):
    if db is None: return
    try:
        db.collection('pdde_contas').document(account_name).set(account_data)
    except Exception as e:
        st.error(f"Erro ao salvar conta: {e}")

def save_empenhos_to_firebase(db, lista_empenhos):
    if db is None: return
    try:
        db.collection('pdde_dados_gerais').document('empenhos').set({'lista': lista_empenhos})
    except Exception as e:
        st.error(f"Erro ao salvar empenhos: {e}")

def save_global_programs_to_firebase(db, lista_programas):
    if db is None: return
    try:
        db.collection('pdde_dados_gerais').document('programas_globais').set({'lista': lista_programas})
    except Exception as e:
        st.error(f"Erro ao salvar programas globais: {e}")

# --- FUNÇÕES AUXILIARES ---
def format_currency(value):
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def init_session_state():
    db = init_firebase()
    st.session_state['db_conn'] = db
    
    if 'accounts' not in st.session_state:
        if db:
            with st.spinner('Conectando ao banco de dados...'):
                st.session_state['accounts'] = load_accounts_from_firebase(db)
        else:
            st.session_state['accounts'] = {}
            
    if 'empenhos_global' not in st.session_state:
        if db:
            st.session_state['empenhos_global'] = load_empenhos_from_firebase(db)
        else:
            st.session_state['empenhos_global'] = []

    if 'global_programs' not in st.session_state:
        if db:
            st.session_state['global_programs'] = load_global_programs_from_firebase(db)
        else:
            st.session_state['global_programs'] = []
        
    if 'available_years' not in st.session_state:
        current_year = datetime.now().year
        anos_encontrados = set([current_year])
        
        for conta in st.session_state['accounts'].values():
            for mov in conta.get('movimentacoes', []):
                anos_encontrados.add(mov.get('ano', current_year))
        
        for emp in st.session_state['empenhos_global']:
            try:
                dt = datetime.strptime(emp['data_empenho'], "%Y-%m-%d")
                anos_encontrados.add(dt.year)
            except:
                pass
                
        st.session_state['available_years'] = sorted(list(anos_encontrados))

def get_saldo_anterior(account_name, programa, tipo_recurso, mes_alvo, ano_alvo):
    conta_data = st.session_state['accounts'][account_name]
    movs = conta_data.get('movimentacoes', []) 
    saldo = 0.0
    saldos_iniciais = conta_data.get('saldos_iniciais', {})
    
    if programa in saldos_iniciais:
        val = saldos_iniciais[programa].get(tipo_recurso, 0.0) if tipo_recurso != 'Total' else \
              saldos_iniciais[programa].get('Capital', 0.0) + saldos_iniciais[programa].get('Custeio', 0.0)
        saldo += val

    for mov in movs:
        mov_ano = mov.get('ano', datetime.now().year)
        mov_mes = mov['mes_num']
        eh_passado = (mov_ano < ano_alvo) or (mov_ano == ano_alvo and mov_mes < mes_alvo)
        
        if mov['programa'] == programa and eh_passado:
            if tipo_recurso == 'Capital':
                saldo += (mov['credito_capital'] + mov['rendimento_capital'] - mov['debito_capital'])
            elif tipo_recurso == 'Custeio':
                saldo += (mov['credito_custeio'] + mov['rendimento_custeio'] - mov['debito_custeio'])
            elif tipo_recurso == 'Total':
                saldo += (mov['total_credito'] + mov['total_rendimento'] - mov['total_debito'])
    return saldo

# --- BARRA LATERAL ---
def sidebar_config():
    st.sidebar.header("⚙️ Configurações Gerais")
    
    if st.session_state['db_conn'] is None:
        st.sidebar.error("⚠️ Sem conexão com Banco de Dados")
    
    st.sidebar.divider()
    st.sidebar.subheader("📍 Navegação")
    modulo_selecionado = st.sidebar.radio(
        "Escolha o Módulo:",
        ["🏦 Movimentação Financeira", "📜 Controle de Empenhos"]
    )
    st.sidebar.divider()

    if modulo_selecionado == "🏦 Movimentação Financeira":
        with st.sidebar.expander("➕ Cadastrar Nova Conta"):
            nova_conta = st.text_input("Número da Conta / Nome", placeholder="Ex: 27.922-6")
            if st.button("Adicionar Conta"):
                if nova_conta and nova_conta not in st.session_state['accounts']:
                    nova_estrutura = {'programas': [], 'movimentacoes': [], 'saldos_iniciais': {}}
                    st.session_state['accounts'][nova_conta] = nova_estrutura
                    save_account_to_firebase(st.session_state['db_conn'], nova_conta, nova_estrutura)
                    st.success(f"Conta {nova_conta} criada!")
                    st.rerun()
                elif nova_conta in st.session_state['accounts']:
                    st.warning("Conta já existe.")

    with st.sidebar.expander("📅 Gerenciar Exercícios (Anos)"):
        novo_ano = st.number_input("Adicionar Ano", min_value=2000, max_value=2050, value=datetime.now().year + 1, step=1)
        if st.button("Criar Novo Exercício"):
            if novo_ano not in st.session_state['available_years']:
                st.session_state['available_years'].append(novo_ano)
                st.session_state['available_years'].sort()
                st.success(f"Exercício de {novo_ano} adicionado!")
                st.rerun()
            else:
                st.warning("Este ano já existe.")
    
    return modulo_selecionado

def calcular_rateio_rendimento(conta, mes_num, ano, rendimento_total_banco, dados_entrada):
    saldos_base = {}
    total_saldo_conta = 0.0
    for prog, valores in dados_entrada.items():
        saldo_ant_cap = get_saldo_anterior(conta, prog, 'Capital', mes_num, ano)
        saldo_ant_cus = get_saldo_anterior(conta, prog, 'Custeio', mes_num, ano)
        base_cap = max(0, saldo_ant_cap + valores['cred_cap'] - valores['deb_cap'])
        base_cus = max(0, saldo_ant_cus + valores['cred_cus'] - valores['deb_cus'])
        saldos_base[prog] = { 'Capital': base_cap, 'Custeio': base_cus }
        total_saldo_conta += (base_cap + base_cus)
    
    resultados = []
    for prog, valores in dados_entrada.items():
        base_prog = saldos_base[prog]
        fator_cap = base_prog['Capital'] / total_saldo_conta if total_saldo_conta > 0 else 0
        fator_cus = base_prog['Custeio'] / total_saldo_conta if total_saldo_conta > 0 else 0
        
        rend_cap = rendimento_total_banco * fator_cap
        rend_cus = rendimento_total_banco * fator_cus
        
        resultados.append({
            'programa': prog, 'mes_num': mes_num, 'ano': ano,
            'credito_capital': valores['cred_cap'], 'credito_custeio': valores['cred_cus'],
            'debito_capital': valores['deb_cap'], 'debito_custeio': valores['deb_cus'],
            'rendimento_capital': rend_cap, 'rendimento_custeio': rend_cus,
            'total_credito': valores['cred_cap'] + valores['cred_cus'],
            'total_debito': valores['deb_cap'] + valores['deb_cus'],
            'total_rendimento': rend_cap + rend_cus
        })
    return resultados

# === VISUALIZAÇÃO 1: MÓDULO FINANCEIRO ===
def render_financeiro_view(conta_atual, ano_atual, programas):
    tab_lanc, tab_rel, tab_resumo = st.tabs(["📝 Lançamentos", "📑 Extrato Mensal", "📊 Resumo Anual"])

    with tab_lanc:
        col_mes, col_rend = st.columns([1, 2])
        meses = {1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho', 
                 7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'}
        
        with col_mes:
            mes_selecionado = st.selectbox("Mês", options=list(meses.keys()), format_func=lambda x: meses[x], key=f"sel_mes_{conta_atual}_{ano_atual}")
        
        movs = st.session_state['accounts'][conta_atual].get('movimentacoes', [])
        registros_existentes = [m for m in movs if m['mes_num'] == mes_selecionado and m.get('ano', datetime.now().year) == ano_atual]
        
        val_rendimento_inicial = 0.0
        if registros_existentes:
            val_rendimento_inicial = sum([m['total_rendimento'] for m in registros_existentes])
            st.info(f"✏️ Editando dados de {meses[mes_selecionado]}.")

        with col_rend:
            rendimento_total = st.number_input(
                "💰 Rendimento/Ajuste (Total Extrato)", 
                value=float(val_rendimento_inicial), step=0.01, format="%.2f", 
                key=f"rend_tot_{conta_atual}_{ano_atual}_{mes_selecionado}"
            )

        st.divider()
        dados_entrada = {}
        for prog in programas:
            prog_data = next((m for m in registros_existentes if m['programa'] == prog), None)
            v_cc = float(prog_data['credito_capital']) if prog_data else 0.0
            v_crc = float(prog_data['credito_custeio']) if prog_data else 0.0
            v_dc = float(prog_data['debito_capital']) if prog_data else 0.0
            v_dec = float(prog_data['debito_custeio']) if prog_data else 0.0

            with st.expander(f"Movimento: {prog}", expanded=True):
                c1, c2, c3, c4 = st.columns(4)
                saldo_ant_cap = get_saldo_anterior(conta_atual, prog, 'Capital', mes_selecionado, ano_atual)
                saldo_ant_cus = get_saldo_anterior(conta_atual, prog, 'Custeio', mes_selecionado, ano_atual)
                
                st.markdown(f"**Saldo Ant.:** Cap: {format_currency(saldo_ant_cap)} | Cust: {format_currency(saldo_ant_cus)}")
                
                k_suf = f"{conta_atual}_{prog}_{ano_atual}_{mes_selecionado}"
                cred_cap = c1.number_input(f"Créd. Capital", min_value=0.0, value=v_cc, key=f"cc_{k_suf}")
                cred_cus = c2.number_input(f"Créd. Custeio", min_value=0.0, value=v_crc, key=f"crc_{k_suf}")
                deb_cap = c3.number_input(f"Déb. Capital", min_value=0.0, value=v_dc, key=f"dc_{k_suf}")
                deb_cus = c4.number_input(f"Déb. Custeio", min_value=0.0, value=v_dec, key=f"dec_{k_suf}")
                dados_entrada[prog] = {'cred_cap': cred_cap, 'cred_cus': cred_cus, 'deb_cap': deb_cap, 'deb_cus': deb_cus}

        if st.button(f"💾 Salvar Lançamento {meses[mes_selecionado]}/{ano_atual}", type="primary", key=f"btn_save_{conta_atual}_{ano_atual}_{mes_selecionado}"):
            novos_registros = calcular_rateio_rendimento(conta_atual, mes_selecionado, ano_atual, rendimento_total, dados_entrada)
            lista_atual = st.session_state['accounts'][conta_atual].get('movimentacoes', [])
            lista_limpa = [m for m in lista_atual if not (m['mes_num'] == mes_selecionado and m.get('ano', datetime.now().year) == ano_atual)]
            lista_limpa.extend(novos_registros)
            st.session_state['accounts'][conta_atual]['movimentacoes'] = lista_limpa
            save_account_to_firebase(st.session_state['db_conn'], conta_atual, st.session_state['accounts'][conta_atual])
            st.success("Dados salvos com sucesso!")
            st.rerun()

    with tab_rel:
        st.subheader(f"Extrato Mensal Detalhado - {ano_atual}")
        filtro_prog = st.selectbox("Filtrar Programa", ["Todos"] + programas, key=f"filt_prog_{conta_atual}_{ano_atual}")
        movs = st.session_state['accounts'][conta_atual].get('movimentacoes', [])
        programas_para_listar = programas if filtro_prog == "Todos" else [filtro_prog]
        
        df_final = pd.DataFrame()
        for p in programas_para_listar:
            dados_tabela = []
            saldo_acumulado_cap = get_saldo_anterior(conta_atual, p, 'Capital', 1, ano_atual)
            saldo_acumulado_cus = get_saldo_anterior(conta_atual, p, 'Custeio', 1, ano_atual)
            
            movs_prog_ano = [m for m in movs if m['programa'] == p and m.get('ano', datetime.now().year) == ano_atual]
            movs_prog_ano.sort(key=lambda x: x['mes_num'])
            
            for m in movs_prog_ano:
                saldo_acumulado_cap += (m['credito_capital'] + m['rendimento_capital'] - m['debito_capital'])
                saldo_acumulado_cus += (m['credito_custeio'] + m['rendimento_custeio'] - m['debito_custeio'])
                saldo_total = saldo_acumulado_cap + saldo_acumulado_cus
                dados_tabela.append({
                    "Programa": p, "Mês": meses[m['mes_num']],
                    "Crédito": m['total_credito'], "Rend. Cap.": m['rendimento_capital'],
                    "Rend. Cust.": m['rendimento_custeio'], "Rend. Total": m['total_rendimento'],
                    "Débito": m['total_debito'], "S. Custeio": saldo_acumulado_cus,
                    "S. Capital": saldo_acumulado_cap, "S. Total": saldo_total
                })
            
            if dados_tabela:
                df_prog = pd.DataFrame(dados_tabela)
                linha_total = pd.DataFrame([{
                    "Programa": "TOTAL", "Mês": "---",
                    "Crédito": df_prog["Crédito"].sum(), "Rend. Cap.": df_prog["Rend. Cap."].sum(),
                    "Rend. Cust.": df_prog["Rend. Cust."].sum(), "Rend. Total": df_prog["Rend. Total"].sum(),
                    "Débito": df_prog["Débito"].sum(), "S. Custeio": df_prog["S. Custeio"].iloc[-1],
                    "S. Capital": df_prog["S. Capital"].iloc[-1], "S. Total": df_prog["S. Total"].iloc[-1]
                }])
                df_final = pd.concat([df_final, df_prog, linha_total], ignore_index=True)

        if not df_final.empty:
            def highlight_total(row):
                return ['background-color: #ffd700; color: black; font-weight: bold'] * len(row) if row['Programa'] == 'TOTAL' else [''] * len(row)
            st.dataframe(df_final.style.format({
                    "Crédito": "R$ {:,.2f}", "Rend. Cap.": "R$ {:,.2f}", "Rend. Cust.": "R$ {:,.2f}",
                    "Rend. Total": "R$ {:,.2f}", "Débito": "R$ {:,.2f}", "S. Custeio": "R$ {:,.2f}",
                    "S. Capital": "R$ {:,.2f}", "S. Total": "R$ {:,.2f}",
                }).apply(highlight_total, axis=1), use_container_width=True, height=500)
        else:
            st.info(f"Nenhuma movimentação em {ano_atual}.")
    
    with tab_resumo:
        st.subheader(f"Resumo Geral das Contas - Exercício {ano_atual}")
        dados_resumo = []
        movs = st.session_state['accounts'][conta_atual].get('movimentacoes', [])
        col_saldo_ant = f"Saldo {ano_atual-1}"
        col_credito = f"Crédito {ano_atual}"
        col_rend = f"Rendimentos {ano_atual}"
        col_debito = f"Débitos {ano_atual}"
        col_saldo_final = f"Saldo 31.12.{ano_atual}"

        for prog in programas:
            saldo_anterior = get_saldo_anterior(conta_atual, prog, 'Total', 1, ano_atual)
            movs_ano = [m for m in movs if m['programa'] == prog and m.get('ano') == ano_atual]
            credito_ano = sum(m['total_credito'] for m in movs_ano)
            rendimento_ano = sum(m['total_rendimento'] for m in movs_ano)
            debito_ano = sum(m['total_debito'] for m in movs_ano)
            saldo_final = saldo_anterior + credito_ano + rendimento_ano - debito_ano
            dados_resumo.append({
                "Programas": prog, col_saldo_ant: saldo_anterior, col_credito: credito_ano,
                col_rend: rendimento_ano, col_debito: debito_ano, col_saldo_final: saldo_final
            })
            
        if dados_resumo:
            df_resumo = pd.DataFrame(dados_resumo)
            linha_total = {
                "Programas": "TOTAL GERAL", col_saldo_ant: df_resumo[col_saldo_ant].sum(),
                col_credito: df_resumo[col_credito].sum(), col_rend: df_resumo[col_rend].sum(),
                col_debito: df_resumo[col_debito].sum(), col_saldo_final: df_resumo[col_saldo_final].sum()
            }
            df_resumo = pd.concat([df_resumo, pd.DataFrame([linha_total])], ignore_index=True)
            def highlight_total_resumo(row):
                return ['background-color: #ffd700; color: black; font-weight: bold'] * len(row) if row['Programas'] == 'TOTAL GERAL' else [''] * len(row)
            st.dataframe(df_resumo.style.format({
                    col_saldo_ant: "R$ {:,.2f}", col_credito: "R$ {:,.2f}", col_rend: "R$ {:,.2f}", 
                    col_debito: "R$ {:,.2f}", col_saldo_final: "R$ {:,.2f}"
                }).apply(highlight_total_resumo, axis=1), use_container_width=True, height=500)
        else:
            st.info("Sem dados para gerar resumo.")

# === VISUALIZAÇÃO 2: MÓDULO EMPENHOS (GLOBAL) ===
def render_empenhos_global_view():
    st.subheader("📜 Controle de Empenhos e Ordens de Pagamento (Global)")
    
    # 1. Gerenciamento de Programas para Empenho
    with st.expander("⚙️ Cadastrar/Gerenciar Programas"):
        c_p1, c_p2 = st.columns([3, 1])
        novo_prog_global = c_p1.text_input("Novo Programa", key="new_prog_global")
        if c_p2.button("Cadastrar", key="btn_add_prog_global"):
            if novo_prog_global and novo_prog_global not in st.session_state['global_programs']:
                st.session_state['global_programs'].append(novo_prog_global)
                save_global_programs_to_firebase(st.session_state['db_conn'], st.session_state['global_programs'])
                st.success("Programa cadastrado!")
                st.rerun()
            elif novo_prog_global:
                st.warning("Programa já existe.")
        
        if st.session_state['global_programs']:
            st.write("Programas cadastrados: " + ", ".join(st.session_state['global_programs']))
        else:
            st.info("Nenhum programa cadastrado para empenhos.")

    # SISTEMA DE EDIÇÃO / CRIAÇÃO
    todos_empenhos = st.session_state['empenhos_global']
    
    # 2. Seleção de Modo (Novo ou Editar Existente)
    opcoes_edicao = ["➕ Novo Registro"]
    mapa_ids = {} # Mapeia o texto do selectbox para o ID real
    
    # Ordena por data (mais recente primeiro)
    todos_empenhos_sorted = sorted(todos_empenhos, key=lambda x: x.get('data_empenho', ''), reverse=True)
    
    for emp in todos_empenhos_sorted:
        try:
            d_fmt = datetime.strptime(emp.get('data_empenho', ''), "%Y-%m-%d").strftime("%d/%m/%Y")
        except:
            d_fmt = "??/??/????"
        
        label = f"{d_fmt} | {emp.get('programa', '?')} | Doc: {emp.get('numero_empenho', '?')}"
        opcoes_edicao.append(label)
        mapa_ids[label] = emp.get('id')

    st.divider()
    escolha = st.selectbox("O que você deseja fazer?", options=opcoes_edicao, key="sel_acao_emp")

    # Estados do Formulário
    dados_edicao = {}
    is_edit_mode = False
    
    if escolha != "➕ Novo Registro":
        is_edit_mode = True
        id_selecionado = mapa_ids[escolha]
        for emp in todos_empenhos:
            if emp.get('id') == id_selecionado:
                dados_edicao = emp
                break
    
    # Helper para garantir data válida
    def safe_date(date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except:
            return datetime.now().date()

    # Prepara valores iniciais (Vazio se novo, preenchido se editar)
    val_prog = dados_edicao.get('programa') if is_edit_mode else None
    val_num = dados_edicao.get('numero_empenho', "")
    val_data = safe_date(dados_edicao.get('data_empenho')) if is_edit_mode else datetime.now().date()
    val_ob = dados_edicao.get('ordem_bancaria', "")
    val_data_ob = safe_date(dados_edicao.get('data_ob')) if is_edit_mode else datetime.now().date()
    val_valor = float(dados_edicao.get('valor', 0.0))
    # Tenta pegar 'data_nota_fiscal', se não existir pega o antigo 'data_utilizacao'
    val_data_nf = safe_date(dados_edicao.get('data_nota_fiscal', dados_edicao.get('data_utilizacao'))) if is_edit_mode else datetime.now().date()
    val_status = dados_edicao.get('status', "PENDENTE")
    val_obs = dados_edicao.get('observacao', "")
    val_itens = dados_edicao.get('itens', "")

    lista_programas = st.session_state['global_programs']
    if not lista_programas:
        lista_programas = ["Sem cadastro"]
    
    # Ajusta índice do programa
    prog_index = 0
    if is_edit_mode and val_prog in lista_programas:
        prog_index = lista_programas.index(val_prog)

    # 3. Formulário Unificado
    with st.container(border=True):
        form_title = "✏️ Editando Registro" if is_edit_mode else "➕ Novo Lançamento"
        st.markdown(f"**{form_title}**")
        
        ce1, ce2, ce3 = st.columns(3)
        e_prog = ce1.selectbox("Programa", options=lista_programas, index=prog_index, key="ge_prog")
        e_num = ce2.text_input("Nº Empenho", value=val_num, key="ge_num")
        # DATA FORMATADA DD/MM/AAAA
        e_data = ce3.date_input("Data do Empenho", value=val_data, format="DD/MM/YYYY", key="ge_data")
        
        ce4, ce5, ce6 = st.columns(3)
        e_ob = ce4.text_input("Ordem Bancária (OB)", value=val_ob, key="ge_ob")
        e_data_ob = ce5.date_input("Data da OB", value=val_data_ob, format="DD/MM/YYYY", key="ge_data_ob")
        e_valor = ce6.number_input("Valor (R$)", value=val_valor, min_value=0.0, step=0.01, format="%.2f", key="ge_valor")
        
        ce7, ce8, ce9 = st.columns(3)
        # NOME CORRIGIDO: Data Nota Fiscal
        e_data_nf = ce7.date_input("Data Nota Fiscal", value=val_data_nf, format="DD/MM/YYYY", key="ge_data_nf")
        e_status = ce8.selectbox("Status", ["EXECUTADO", "PENDENTE", "CANCELADO"], index=["EXECUTADO", "PENDENTE", "CANCELADO"].index(val_status), key="ge_status")
        e_obs = ce9.text_input("Observação", value=val_obs, placeholder="Ex: 1ª Parcela", key="ge_obs")
        
        e_itens = st.text_area("Itens Comprados / Descrição", value=val_itens, placeholder="Ex: Arroz, Feijão...", key="ge_itens")
        
        col_btn1, col_btn2 = st.columns([1, 5])
        
        if is_edit_mode:
            # BOTÕES DE EDIÇÃO
            if col_btn1.button("💾 Atualizar", type="primary", key="btn_upd_gemp"):
                # Encontra o índice na lista original para atualizar
                idx = -1
                for i, emp in enumerate(st.session_state['empenhos_global']):
                    if emp.get('id') == id_selecionado:
                        idx = i
                        break
                
                if idx != -1:
                    st.session_state['empenhos_global'][idx].update({
                        "programa": e_prog, "numero_empenho": e_num,
                        "data_empenho": e_data.strftime("%Y-%m-%d"),
                        "ordem_bancaria": e_ob, "data_ob": e_data_ob.strftime("%Y-%m-%d"),
                        "valor": e_valor, 
                        "data_nota_fiscal": e_data_nf.strftime("%Y-%m-%d"), # Chave atualizada
                        "status": e_status, "itens": e_itens, "observacao": e_obs
                    })
                    save_empenhos_to_firebase(st.session_state['db_conn'], st.session_state['empenhos_global'])
                    st.success("Registro atualizado com sucesso!")
                    st.rerun()
            
            if col_btn2.button("🗑️ Excluir", type="secondary", key="btn_del_gemp"):
                idx = -1
                for i, emp in enumerate(st.session_state['empenhos_global']):
                    if emp.get('id') == id_selecionado:
                        idx = i
                        break
                if idx != -1:
                    st.session_state['empenhos_global'].pop(idx)
                    save_empenhos_to_firebase(st.session_state['db_conn'], st.session_state['empenhos_global'])
                    st.success("Registro excluído!")
                    st.rerun()
        else:
            # BOTÃO DE CRIAÇÃO
            if st.button("Salvar Novo Empenho", type="primary", key="btn_save_gemp"):
                novo_empenho = {
                    "id": str(datetime.now().timestamp()),
                    "programa": e_prog, "numero_empenho": e_num,
                    "data_empenho": e_data.strftime("%Y-%m-%d"),
                    "ordem_bancaria": e_ob, "data_ob": e_data_ob.strftime("%Y-%m-%d"),
                    "valor": e_valor, 
                    "data_nota_fiscal": e_data_nf.strftime("%Y-%m-%d"), # Chave atualizada
                    "status": e_status, "itens": e_itens, "observacao": e_obs
                }
                
                st.session_state['empenhos_global'].append(novo_empenho)
                save_empenhos_to_firebase(st.session_state['db_conn'], st.session_state['empenhos_global'])
                st.success("Empenho salvo na lista geral!")
                st.rerun()

    # 4. Tabela de Visualização
    st.divider()
    
    anos_disp = sorted(st.session_state.get('available_years', [datetime.now().year]))
    str_anos = [str(a) for a in anos_disp]
    ano_filtro = st.radio("Filtrar por Ano:", str_anos, horizontal=True, index=len(str_anos)-1)
    
    st.markdown(f"**Lista de Empenhos - {ano_filtro}**")
    
    empenhos_ano = []
    
    for emp in todos_empenhos:
        try:
            dt = datetime.strptime(emp['data_empenho'], "%Y-%m-%d")
            if str(dt.year) == ano_filtro:
                empenhos_ano.append(emp)
        except:
            pass
    
    if empenhos_ano:
        filtro_prog_emp = st.selectbox("Filtrar por Programa", ["Todos"] + lista_programas, key="filt_gemp")
        lista_final = empenhos_ano
        if filtro_prog_emp != "Todos":
            lista_final = [e for e in empenhos_ano if e['programa'] == filtro_prog_emp]
        
        if lista_final:
            tabela_dados = []
            for item in lista_final:
                d_emp = datetime.strptime(item['data_empenho'], "%Y-%m-%d").strftime("%d/%m/%Y")
                d_ob = datetime.strptime(item['data_ob'], "%Y-%m-%d").strftime("%d/%m/%Y")
                
                # Tenta pegar a data nota fiscal, se não tiver (dados antigos), pega utilização
                raw_nf = item.get('data_nota_fiscal', item.get('data_utilizacao', ''))
                try:
                    d_nf = datetime.strptime(raw_nf, "%Y-%m-%d").strftime("%d/%m/%Y")
                except:
                    d_nf = ""

                tabela_dados.append({
                    "Programa": item['programa'], "Nº Empenho": item['numero_empenho'], "Data": d_emp,
                    "OB": item['ordem_bancaria'], "Data OB": d_ob, "Valor": item['valor'],
                    "Data NF": d_nf, "Itens": item['itens'], "Obs": item['observacao'], "Status": item['status']
                })
            df_emp = pd.DataFrame(tabela_dados)
            st.dataframe(df_emp.style.format({"Valor": "R$ {:,.2f}"}), use_container_width=True, height=400)
            
            total_empenhado = df_emp['Valor'].sum()
            col_met1, col_met2 = st.columns(2)
            col_met1.metric("Total Empenhado (Filtro)", format_currency(total_empenhado))
            col_met2.metric("Quantidade de Registros", len(df_emp))
        else:
            st.info(f"Nenhum empenho encontrado para {filtro_prog_emp} em {ano_filtro}.")
    else:
        st.info(f"Nenhum empenho registrado em {ano_filtro}.")

def main():
    init_session_state()
    modulo_selecionado = sidebar_config()
    
    st.title(f"📊 {modulo_selecionado}")
    
    if modulo_selecionado == "🏦 Movimentação Financeira":
        contas = list(st.session_state['accounts'].keys())
        if not contas:
            st.info("👈 Cadastre uma conta na barra lateral para começar a usar o financeiro.")
            return

        for aba, nome in zip(st.tabs(contas), contas):
            with aba:
                st.header(f"Conta: {nome}")
                
                with st.expander("⚙️ Gerenciar Programas da Conta"):
                    c1, c2 = st.columns([3, 1])
                    novo = c1.text_input("Novo Programa", key=f"np_{nome}")
                    if c2.button("Adicionar", key=f"b_{nome}"):
                        if novo and novo not in st.session_state['accounts'][nome]['programas']:
                            st.session_state['accounts'][nome]['programas'].append(novo)
                            if 'saldos_iniciais' not in st.session_state['accounts'][nome]:
                                st.session_state['accounts'][nome]['saldos_iniciais'] = {}
                            st.session_state['accounts'][nome]['saldos_iniciais'][novo] = {'Capital': 0.0, 'Custeio': 0.0}
                            save_account_to_firebase(st.session_state['db_conn'], nome, st.session_state['accounts'][nome])
                            st.rerun()
                    
                    progs = st.session_state['accounts'][nome].get('programas', [])
                    if progs:
                        st.write("---")
                        st.write("Saldos Iniciais (Abertura de Conta):")
                        for p in progs:
                            si = st.session_state['accounts'][nome].setdefault('saldos_iniciais', {}).setdefault(p, {'Capital': 0.0, 'Custeio': 0.0})
                            k = f"{nome}_{p}"
                            cols = st.columns([2, 1, 1, 1])
                            cols[0].write(f"📂 {p}")
                            n_cap = cols[1].number_input("Saldo Inicial Capital", value=si['Capital'], key=f"sic_{k}")
                            n_cus = cols[2].number_input("Saldo Inicial Custeio", value=si['Custeio'], key=f"sis_{k}")
                            if cols[3].button("Salvar", key=f"bts_{k}"):
                                si['Capital'] = n_cap
                                si['Custeio'] = n_cus
                                save_account_to_firebase(st.session_state['db_conn'], nome, st.session_state['accounts'][nome])
                                st.rerun()

                if st.session_state['accounts'][nome]['programas']:
                    anos = sorted(st.session_state.get('available_years', [datetime.now().year]))
                    for aba_ano, ano in zip(st.tabs([str(a) for a in anos]), anos):
                        with aba_ano:
                            render_financeiro_view(nome, ano, st.session_state['accounts'][nome]['programas'])
                else:
                    st.warning("Cadastre programas acima para começar.")
    
    else:
        render_empenhos_global_view()

if __name__ == "__main__":
    main()