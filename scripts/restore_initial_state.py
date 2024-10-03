#!/usr/bin/env python

"""This script restores the default TAX codes, accounts, and journals in a CashCtrl ledger system.

Automatic Execution:
    This script is set to run automatically every night at 3 AM as a GitHub Action.
    This ensures that the CashCtrl ledger is reset to its default state daily.

Manual Execution:
    - Users can also run this script locally from the terminal.
      This is useful for testing purposes or when an immediate
      reset is required outside the scheduled GitHub Action.
    - Can be triggered in the GitHub actions interface using [Run workflow] button

Usage:
    To run the script locally, use the following command in your terminal:
    ```sh
    CC_API_ORGANISATION=<myorg> CC_API_KEY=<myapikey> scripts/restore_initial_state.py
    ```
"""

from io import StringIO
from cashctrl_ledger import CashCtrlLedger
import pandas as pd

TAX_CODES = """
    id,account,rate,is_inclusive,description
    TAX 2.6%,2200,0.026,,TAX 2.6%
    TAX 3.8%,2200,0.038,,TAX 3.8%
    TAX 8.1%,2200,0.081,,TAX 8.1%
    Input tax 2.6%,1170,0.026,,Input tax 2.6%
    Input tax 3.8%,1170,0.038,,Input tax 3.8%
    Input tax 8.1%,1170,0.081,,Input tax 8.1%
"""

# flake8: noqa: E501

ACCOUNTS = """
    account,currency,description,tax_code,group
    1000,CHF,<values><de>Kasse</de><en>Cash</en><fr>Caisse</fr><it>Cassa</it></values>,,/Assets/Umlaufvermögen/Flüssige Mittel
    1010,CHF,<values><de>Post</de><en>Postal account</en><fr>Poste</fr><it>Posta</it></values>,,/Assets/Umlaufvermögen/Flüssige Mittel
    1020,CHF,<values><de>Bank</de><en>Bank account</en><fr>Compte courant</fr><it>Conto corrente</it></values>,,/Assets/Umlaufvermögen/Flüssige Mittel
    1060,CHF,<values><de>Wertschriften</de><en>Marketable securities</en><fr>Actions</fr><it>Azioni</it></values>,,/Assets/Umlaufvermögen/Kurzfristige Wertschriften
    1100,CHF,<values><de>Debitoren</de><en>Accounts receivable</en><fr>Créances</fr><it>Crediti</it></values>,,"/Assets/Umlaufvermögen/Forderungen Lieferungen, Leistungen"
    1109,CHF,<values><de>Delkredere</de><en>Allowance for doubtful accounts</en><fr>Ducroire</fr><it>Delcredere</it></values>,,"/Assets/Umlaufvermögen/Forderungen Lieferungen, Leistungen"
    1140,CHF,<values><de>Vorschüsse und Darlehen</de><en>Advances and loans</en><fr>Prêts</fr><it>Prestiti</it></values>,,/Assets/Umlaufvermögen/Übrige kurzfristige Forderungen
    1170,CHF,<values><de>Vorsteuer</de><en>Sales tax receivable</en><fr>Impôt préalable</fr><it>Imposta precedente</it></values>,,/Assets/Umlaufvermögen/Übrige kurzfristige Forderungen
    1172,CHF,<values><de>Vorsteuerausgleich Abrechnungsmethode</de><en>Input tax compensation settlement method</en><fr>Réconciliation de l'impôt préalable</fr><it>Compensazione della deduzione dell'imposta precedente</it></values>,,/Assets/Umlaufvermögen/Übrige kurzfristige Forderungen
    1176,CHF,<values><de>Verrechnungssteuer</de><en>Withholding tax receivable</en><fr>Impôt anticipé à récupérer</fr><it>Imposta preventiva</it></values>,,/Assets/Umlaufvermögen/Übrige kurzfristige Forderungen
    1180,CHF,<values><de>Debitor Sozialversicherungen</de><en>Social security receivable</en><fr>Compte courant assurances sociales</fr><it>Conto corrente sicurezza sociale</it></values>,,/Assets/Umlaufvermögen/Übrige kurzfristige Forderungen
    1190,CHF,<values><de>Sonstige kurzfristige Forderungen</de><en>Other short-term receivables</en><fr>Autres créances à court terme</fr><it>Altri crediti correnti</it></values>,,/Assets/Umlaufvermögen/Übrige kurzfristige Forderungen
    1200,CHF,<values><de>Handelswaren</de><en>Trade goods</en><fr>Stocks de marchandise</fr><it>Merce di rivendita</it></values>,,/Assets/Umlaufvermögen/Vorräte
    1210,CHF,<values><de>Rohstoffe</de><en>Raw materials</en><fr>Stocks de matières premières</fr><it>Meteria prima</it></values>,,/Assets/Umlaufvermögen/Vorräte
    1260,CHF,<values><de>Fertige Erzeugnisse</de><en>Finished products</en><fr>Produits fini</fr><it>Prodotti finiti</it></values>,,/Assets/Umlaufvermögen/Vorräte
    1270,CHF,<values><de>Unfertige Erzeugnisse</de><en>Unfinished products</en><fr>Produits semi-ouvrés</fr><it>Prodotti in corso di fabbricazione</it></values>,,/Assets/Umlaufvermögen/Vorräte
    1280,CHF,<values><de>Nicht fakturierte Dienstleistungen</de><en>Services not yet invoiced</en><fr>Traveaux en cours</fr><it>Prestazioni di servizi non fatturate</it></values>,,/Assets/Umlaufvermögen/Vorräte
    1300,CHF,<values><de>Bezahlter Aufwand des Folgejahres</de><en>Prepaid expenses</en><fr>Charges payées d'avance</fr><it>Costi dell'anno successivo pagati in anticipo</it></values>,,/Assets/Umlaufvermögen/Abgrenzungen
    1301,CHF,<values><de>Noch nicht erhaltener Ertrag</de><en>Revenue not yet received</en><fr>Produits à recevoir</fr><it>Ricavi non ancora ricevuti</it></values>,,/Assets/Umlaufvermögen/Abgrenzungen
    1400,CHF,<values><de>Wertschriften</de><en>Securities</en><fr>Titres</fr><it>Titoli</it></values>,,/Assets/Anlagevermögen/Finanzanlagen
    1430,CHF,<values><de>Andere Finanzanlagen</de><en>Other financial assets</en><fr>Autres placements</fr><it>Altre immobilizzazioni finanziarie</it></values>,,/Assets/Anlagevermögen/Finanzanlagen
    1440,CHF,<values><de>Darlehen</de><en>Loans</en><fr>Prêts</fr><it>Prestiti</it></values>,,/Assets/Anlagevermögen/Finanzanlagen
    1441,CHF,<values><de>Hypotheken</de><en>Mortgages</en><fr>Hypothèques</fr><it>Prestiti ipotecari</it></values>,,/Assets/Anlagevermögen/Finanzanlagen
    1480,CHF,<values><en>Holdings</en><de>Beteiligungen</de><fr>Participations</fr><it>Partecipazioni</it></values>,,/Assets/Anlagevermögen
    1500,CHF,<values><de>Maschinen und Apparate</de><en>Machinery and production plants</en><fr>Machines et appareils</fr><it>Macchine e attrezzature</it></values>,,/Assets/Anlagevermögen/Mobile Sachanlagen
    1510,CHF,<values><de>Mobiliar und Einrichtungen</de><en>Furniture and equipment</en><fr>Mobilier et installations</fr><it>Mobilio e installazioni</it></values>,,/Assets/Anlagevermögen/Mobile Sachanlagen
    1520,CHF,"<values><de>Büromaschinen, IT, Kommunikation</de><en>Office machines, computers</en><fr>Machines de bureau, informatique, systèmes de communication</fr><it>Macchine ufficio, informatica, e technologica della comunicazione</it></values>",,/Assets/Anlagevermögen/Mobile Sachanlagen
    1530,CHF,<values><de>Fahrzeuge</de><en>Vehicles</en><fr>Véhicules</fr><it>Veicoli</it></values>,,/Assets/Anlagevermögen/Mobile Sachanlagen
    1540,CHF,<values><de>Werkzeuge und Geräte</de><en>Tools</en><fr>Outillage et appareils</fr><it>Utensili e apparecchiature</it></values>,,/Assets/Anlagevermögen/Mobile Sachanlagen
    1600,CHF,<values><de>Geschäftsimmobilien</de><en>Commercial properties</en><fr>Immeubles d'exploitation</fr><it>Immobili aziendali</it></values>,,/Assets/Anlagevermögen/Immobile Sachanlagen
    1700,CHF,"<values><de>Patente, Lizenzen, Rechte</de><en>Patents, licenses, copyrights</en><fr>Brevets, licences, droits de propriété intellectuelle</fr><it>Brevetti, licenze, diritti d'autore</it></values>",,/Assets/Anlagevermögen/Immaterielle Werte
    1770,CHF,<values><de>Goodwill</de><en>Goodwill</en><fr>Goodwill</fr><it>Goodwill</it></values>,,/Assets/Anlagevermögen/Immaterielle Werte
    1850,CHF,<values><de>Nicht einbezahltes Kapital</de><en>Not paid-in capital</en><fr>Capital non libéré</fr><it>Capitale non versati</it></values>,,/Assets/Anlagevermögen/Nicht einbezahltes Kapital
    2000,CHF,<values><en>Accounts payable</en><de>Kreditoren</de><fr>Créanciers</fr><it>Creditori</it></values>,,/Liabilities/Langfristiges Fremdkapital/Verzinsliche Verbindlichkeiten
    2030,CHF,<values><en>Advances received</en><de>Erhaltene Anzahlungen</de><fr>Acomptes reçus de tiers</fr><it>Anticipi ricevuti da terzi</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2100,CHF,<values><en>Short-term bank liabilities</en><de>Bankverbindlichkeiten kurzfristig</de><fr>Dettes bancaires à court terme</fr><it>Debiti verso banche a breve termine</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2120,CHF,<values><en>Short-term financial leasing</en><de>Finanzierungsleasing kurzfristig</de><fr>Engagements de financement par leasing à court terme</fr><it>Impegni da leasing finanziari a breve termine</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2140,CHF,<values><en>Other interest-bearing liabilities</en><de>Übrige verzinsliche Verbindlichkeiten</de><fr>Autres dettes portant intérêt envers des tiers</fr><it>Altri debiti onerosi verso terzi</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2200,CHF,<values><en>Sales tax payable</en><de>Umsatzsteuer</de><fr>TVA due</fr><it>IVA dovuta</it></values>,,/Liabilities/Langfristiges Fremdkapital/Übrige Verbindlichkeiten
    2202,CHF,<values><en>Tax compensation settlement method</en><de>Umsatzsteuerausgleich Abrechnungsmethode</de><fr>Réconciliation du chiffre d'affaires suite à un changement de methode</fr><it>Compensazione IVA dovuta in base al metodo di rendiconto</it></values>,,/Liabilities/Langfristiges Fremdkapital/Übrige Verbindlichkeiten
    2206,CHF,<values><en>Withholding tax payable</en><de>Verrechnungssteuer</de><fr>Impôt anticipé à payer</fr><it>Imposta preventiva</it></values>,,/Liabilities/Langfristiges Fremdkapital/Übrige Verbindlichkeiten
    2208,CHF,<values><en>Direct taxes</en><de>Direkte Steuern</de><fr>Impôts directs</fr><it>Imposte dirette</it></values>,,/Liabilities/Langfristiges Fremdkapital/Übrige Verbindlichkeiten
    2210,CHF,<values><en>Other short-term liabilities</en><de>Sonstige kurzfristige Verbindlichkeiten</de><fr>Autres dettes à court terme envers des tiers</fr><it>Altri debiti a breve termine verso terzi</it></values>,,/Liabilities/Langfristiges Fremdkapital/Übrige Verbindlichkeiten
    2261,CHF,<values><en>Profit distribution</en><de>Beschlossene Ausschüttungen</de><fr>Dividendes</fr><it>Dividendi</it></values>,,/Liabilities/Langfristiges Fremdkapital/Übrige Verbindlichkeiten
    2270,CHF,<values><en>Social security payable</en><de>Sozialversicherungen</de><fr>Assurances sociales</fr><it>Sicurezza sociale</it></values>,,/Liabilities/Langfristiges Fremdkapital/Übrige Verbindlichkeiten
    2279,CHF,<values><en>Withholding tax</en><de>Quellensteuer</de><fr>Compte courant Impôt à la source</fr><it>Conto corrente imposta alla fonte</it></values>,,/Liabilities/Langfristiges Fremdkapital/Verzinsliche Verbindlichkeiten
    2300,CHF,<values><en>Expenses not yet paid</en><de>Noch nicht bezahlter Aufwand</de><fr>Charges à payer</fr><it>Costi da pagare</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2301,CHF,<values><en>Unearned revenue</en><de>Erhaltener Ertrag des Folgejahres</de><fr>Produits encaissés d'avance</fr><it>Ricavi ricevuti dell'anno successivo</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2330,CHF,<values><en>Short-term provisions</en><de>Kurzfristige Rückstellungen</de><fr>Provisions à court terme</fr><it>Accantonamenti a breve termine</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2400,CHF,<values><de>Bankverbindlichkeiten langfristig</de><en>Long-term bank liabilities</en><fr>Dettes bancaires à long terme</fr><it>Debiti verso banche a lungo termine</it></values>,,/Liabilities/Langfristiges Fremdkapital/Verzinsliche Verbindlichkeiten
    2420,CHF,<values><de>Finanzierungsleasing langfristig</de><en>Long-term financial leasing</en><fr>Engagement de financement par leasing à long terme</fr><it>Impegni da leasing finanziari a lungo termine</it></values>,,/Liabilities/Langfristiges Fremdkapital/Verzinsliche Verbindlichkeiten
    2430,CHF,<values><de>Obligationenanleihen</de><en>Bonds payable</en><fr>Emprunts obligatoires</fr><it>Prestiti obbligazionari</it></values>,,/Liabilities/Langfristiges Fremdkapital/Verzinsliche Verbindlichkeiten
    2450,CHF,<values><de>Darlehen</de><en>Loans</en><fr>Emprunts</fr><it>Prestiti</it></values>,,/Liabilities/Langfristiges Fremdkapital/Verzinsliche Verbindlichkeiten
    2451,CHF,<values><de>Hypotheken</de><en>Mortgages</en><fr>Hypothèques</fr><it>Mutui ipotecari</it></values>,,/Liabilities/Langfristiges Fremdkapital/Verzinsliche Verbindlichkeiten
    2500,CHF,<values><de>Unverzinsliche langfristige Verbindlichkeiten</de><en>Non-interest-bearing long-term liabilities</en><fr>Autres dettes à long terme á l'égard de tiers sans intérêts</fr><it>Altri debiti a lungo termine verso terzi non onerosi</it></values>,,/Liabilities/Langfristiges Fremdkapital/Übrige Verbindlichkeiten
    2600,CHF,<values><de>Langfristige Rückstellungen</de><en>Long-term provisions</en><fr>Provisions à long terme</fr><it>Accantonamenti a lungo termine</it></values>,,/Liabilities/Langfristiges Fremdkapital/Rückstellungen
    2800,CHF,"<values><en>Nominal capital, Share capital</en><de>Stammkapital, Aktienkapital</de><fr>Capital social, Capital-actions</fr><it>Capitale sociale, Capitale azionario</it></values>",,/Liabilities/Langfristiges Fremdkapital
    2820,CHF,<values><en>Private equity (partner)</en><de>Privat Gesellschafter</de><fr>Capital propre associé</fr><it>Capitale proprio socio</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2850,CHF,<values><en>Private equity (sole entrepreneur)</en><de>Privat Einzelunternehmer</de><fr>Capital propre (raison individuelle)</fr><it>Capitale proprio (ditte individuali)</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2900,CHF,<values><en>Statutory capital reserves</en><de>Gesetzliche Kapitalreserve</de><fr>Réserve légale issue du capital</fr><it>Riserva legale da capitale</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2930,CHF,"<values><en>Reserve for own shares</en><de>Reserve für eigene Kapitalanteile</de><fr>Réserve propres actions, parts sociales</fr><it>Riserva proprie quote di capitale</it></values>",,/Liabilities/Langfristiges Fremdkapital
    2940,CHF,<values><en>Revaluation reserve</en><de>Aufwertungsreserve</de><fr>Réserve d'évaluation</fr><it>Riserva da rivalutazioni</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2950,CHF,<values><en>Statutory retained earnings</en><de>Gesetzliche Gewinnreserve</de><fr>Réserve légale issue du bénéfice</fr><it>Risserva legale da utili</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2960,CHF,<values><en>Voluntary retained earnings</en><de>Freiwillige Gewinnreserven</de><fr>Réserves facultative</fr><it>Riserva facultative</it></values>,,/Liabilities/Langfristiges Fremdkapital
    2979,CHF,<values><en>Annual profit or loss</en><de>Jahresgewinn oder -verlust</de><fr>Bénéfice ou perte de l'exercice</fr><it>Utile annuo o perdita annua</it></values>,,/Liabilities/Langfristiges Fremdkapital
    3000,CHF,<values><de>Produktionsertrag</de><en>Production revenue</en><fr>Ventes de produits</fr><it>Ricavi produzione</it></values>,,/Revenue/Betriebsertrag
    3200,CHF,<values><de>Handelsertrag</de><en>Trading revenue</en><fr>Produits de marchandises</fr><it>Ricavi merci di rivendita</it></values>,,/Revenue/Betriebsertrag
    3400,CHF,<values><de>Dienstleistungsertrag</de><en>Services revenue</en><fr>Ventes de prestations</fr><it>Ricavi prestazioni di servizi</it></values>,,/Revenue/Betriebsertrag
    3600,CHF,"<values><de>Übriger Ertrag Lieferungen, Leistungen</de><en>Other trading revenue</en><fr>Produits annexes résultant de la vente de biens et de prestations de services</fr><it>Altri ricavi e prestazioni di servizi</it></values>",,/Revenue/Betriebsertrag
    3700,CHF,<values><de>Eigenleistungen</de><en>Personal contributions</en><fr>Prestations propres</fr><it>Lavori interni</it></values>,,/Revenue/Betriebsertrag
    3710,CHF,<values><de>Eigenverbrauch</de><en>Own consumption</en><fr>Propres consommations</fr><it>Consumo proprio</it></values>,,/Revenue/Betriebsertrag
    3800,CHF,<values><de>Ertragsminderung</de><en>Loss of earnings</en><fr>Déductions sur ventes</fr><it>Diminuzioni dei ricavi</it></values>,,/Revenue/Betriebsertrag
    3805,CHF,<values><de>Debitorenverluste</de><en>Losses on accounts receivable</en><fr>Pertes sur créances</fr><it>Perditi sui crediti</it></values>,,/Revenue/Betriebsertrag
    3900,CHF,<values><de>Bestandesänderung unfertige Erzeugnisse</de><en>Inventory change unfinished products</en><fr>Variation des stocks de produits semi-finis</fr><it>Variazioni scorte di prodotti in corso di fabbricazione</it></values>,,/Revenue/Betriebsertrag
    3901,CHF,<values><de>Bestandesänderung fertige Erzeugnisse</de><en>Inventory change finished products</en><fr>Variation des stocks de produits finis</fr><it>Variazioni scorte di prodotti finiti</it></values>,,/Revenue/Betriebsertrag
    3940,CHF,<values><de>Bestandesänderung nicht fakturierte Dienstleistungen</de><en>Inventory change services not yet invoiced</en><fr>Variation de la valeur des prestations non facturées</fr><it>Variazioni scorte di prestazioni di servizi non fatturate</it></values>,,/Revenue/Betriebsertrag
    4000,CHF,<values><de>Materialaufwand</de><en>Cost of materials</en><fr>Charges de matériel</fr><it>Costi per il materiale</it></values>,,"/Expense/Aufwand Lieferungen, Leistungen"
    4200,CHF,<values><de>Warenaufwand</de><en>Cost of goods sold</en><fr>Charges de marchandises</fr><it>Costi delle merci di rivendita</it></values>,,"/Expense/Aufwand Lieferungen, Leistungen"
    4400,CHF,<values><de>Dienstleistungsaufwand</de><en>Cost of services</en><fr>Charges de prestations</fr><it>Costi per lavori</it></values>,,"/Expense/Aufwand Lieferungen, Leistungen"
    4500,CHF,<values><de>Energieaufwand Leistungserstellung</de><en>Energy cost of production</en><fr>Charges d'énergie pour l'exploitation</fr><it>Consumi energia per la produzione</it></values>,,"/Expense/Aufwand Lieferungen, Leistungen"
    4900,CHF,<values><de>Aufwandminderungen</de><en>Expense reductions</en><fr>Déductions des charges</fr><it>Diminuzioni dei costi</it></values>,,"/Expense/Aufwand Lieferungen, Leistungen"
    5000,CHF,<values><de>Lohnaufwand</de><en>Salary expenses</en><fr>Salaires</fr><it>Salari</it></values>,,/Expense/Personalaufwand
    5700,CHF,<values><de>Sozialleistungen</de><en>Social benefits</en><fr>Charges sociales</fr><it>Costi delle assicurazioni sociali</it></values>,,/Expense/Personalaufwand
    5800,CHF,<values><de>Übriger Personalaufwand</de><en>Other personnel expenses</en><fr>Autres charges de personnel</fr><it>Altri costi per il personale</it></values>,,/Expense/Personalaufwand
    5810,CHF,"<values><de>Aus- und Weiterbildung</de><en>Training, education</en><fr>Formation et formation continue</fr><it>Formazione e aggiornamento professionale</it></values>",,/Expense/Personalaufwand
    5820,CHF,<values><de>Spesenentschädigungen</de><en>Expense compensation</en><fr>Indemnités</fr><it>Rimborso spese</it></values>,,/Expense/Personalaufwand
    5900,CHF,<values><de>Leistungen Dritter</de><en>Third-party services</en><fr>Prestations de tiers</fr><it>Prestazioni di terzi</it></values>,,/Expense/Personalaufwand
    6000,CHF,<values><de>Raumaufwand</de><en>Occupancy expenses</en><fr>Charges de loceaux</fr><it>Costi dei locali</it></values>,,/Expense/Übriger Betriebsaufwand
    6100,CHF,"<values><de>Unterhalt, Reparatur, Ersatz</de><en>Maintenance expenses</en><fr>Entretien, réparations, remplacement</fr><it>Manutenzioni, riparazioni, sostituzioni</it></values>",,/Expense/Übriger Betriebsaufwand
    6105,CHF,<values><de>Leasingaufwand mobile Sachanlagen</de><en>Leasing expenses mobile tangible assets</en><fr>Leasing immobilisations corporelles meubles</fr><it>Leasing di immobilizzazioni meteriali mobiliari</it></values>,,/Expense/Übriger Betriebsaufwand
    6200,CHF,<values><de>Fahrzeug- und Transportaufwand</de><en>Vehicle and transportation expenses</en><fr>Charges de véhicules et de transport</fr><it>Costi auto e di transporte</it></values>,,/Expense/Übriger Betriebsaufwand
    6260,CHF,<values><de>Fahrzeugleasing und -miete</de><en>Vehicle leasing and rent</en><fr>Véhicules en leasing</fr><it>Leasing veicoli</it></values>,,/Expense/Übriger Betriebsaufwand
    6300,CHF,"<values><de>Sachversicherungen, Gebühren, Bewilligungen</de><en>Property insurance, fees, permits</en><fr>Assurances-choses, taxes, autorisations</fr><it>Assicurazioni cose, tasse, autorizzazioni</it></values>",,/Expense/Übriger Betriebsaufwand
    6400,CHF,<values><de>Energie- und Entsorgungsaufwand</de><en>Energy and disposal expenses</en><fr>Charges d'énergie et évacuation des déchets</fr><it>Costi energia e smaltimento</it></values>,,/Expense/Übriger Betriebsaufwand
    6500,CHF,<values><de>Verwaltungsaufwand</de><en>Administrative expenses</en><fr>Charges d'administration</fr><it>Costi amministrativi</it></values>,,/Expense/Übriger Betriebsaufwand
    6570,CHF,<values><de>Informatikaufwand</de><en>IT expenses</en><fr>Frais informatiques</fr><it>Costi informatici</it></values>,,/Expense/Übriger Betriebsaufwand
    6600,CHF,<values><de>Werbeaufwand</de><en>Advertising expenses</en><fr>Charges de publicité</fr><it>Costi pubblicitari</it></values>,,/Expense/Übriger Betriebsaufwand
    6700,CHF,<values><de>Übriger Betriebsaufwand</de><en>Other operating expenses</en><fr>Autres charges d'exploitation</fr><it>Altri costi d'esercizio</it></values>,,/Expense/Übriger Betriebsaufwand
    6800,CHF,<values><de>Abschreibungen</de><en>Depreciations</en><fr>Amortissements</fr><it>Ammortamenti</it></values>,,/Expense/Übriger Betriebsaufwand
    6801,CHF,<values><de>Anlagenabgänge</de><en>Fixed asset disposals</en><fr>Cessions d'immobilisations</fr><it>Cessioni di immobilizzazioni</it></values>,,/Expense/Übriger Betriebsaufwand
    6900,CHF,<values><de>Finanzaufwand</de><en>Financial expenses</en><fr>Charges financières</fr><it>Costi finanziari</it></values>,,/Expense/Übriger Betriebsaufwand
    6950,CHF,<values><de>Finanzertrag</de><en>Financial revenue</en><fr>Produits financiers</fr><it>Ricavi finanziari</it></values>,,/Expense/Übriger Betriebsaufwand
    6960,CHF,<values><de>Kursdifferenzen</de><en>Exchange differences</en><fr>Différences de change</fr><it>Differenze di cambio</it></values>,,/Expense/Übriger Betriebsaufwand
    6961,CHF,<values><de>Rundungsdifferenzen</de><en>Rounding differences</en><fr>Différences d'arrondi</fr><it>Differenze di arrotondamento</it></values>,,/Expense/Übriger Betriebsaufwand
    7000,CHF,<values><de>Ertrag Nebenbetrieb</de><en>Revenue from ancillary operations</en><fr>Produits accessoires</fr><it>Ricavi attività accessoria</it></values>,,/Expense/Betrieblicher Nebenerfolg
    7010,CHF,<values><de>Aufwand Nebenbetrieb</de><en>Expenses from ancillary operations</en><fr>Charges accessoires</fr><it>Costi attività accessoria</it></values>,,/Expense/Betrieblicher Nebenerfolg
    7500,CHF,<values><de>Ertrag Immobilien</de><en>Revenue from immobile tangible assets</en><fr>Produits d'immeubles</fr><it>Ricavi immobile</it></values>,,/Expense/Betrieblicher Nebenerfolg
    7510,CHF,<values><de>Aufwand Immobilien</de><en>Expenses from immobile tangible assets</en><fr>Charges d'immeubles</fr><it>Costi immobile</it></values>,,/Expense/Betrieblicher Nebenerfolg
    7900,CHF,<values><de>Ertrag Mobile Sachanlagen</de><en>Revenue from mobile tangible assets</en><fr>Produits des immobilisations corporelles meubles</fr><it>Ricavi da immobilizzazioni materiali mobiliari</it></values>,,/Expense/Betrieblicher Nebenerfolg
    7910,CHF,<values><de>Ertrag Immaterielle Werte</de><en>Revenue from intangible assets</en><fr>Produits des immobilisations incorporelles</fr><it>Ricavi da immobilizzazioni immateriali</it></values>,,/Expense/Betrieblicher Nebenerfolg
    8000,CHF,<values><de>Betriebsfremder Aufwand</de><en>Non-operating expenses</en><fr>Charges hors exploitation</fr><it>Costi estranei</it></values>,,/Expense/Betriebsfremder Erfolg
    8100,CHF,<values><de>Betriebsfremder Ertrag</de><en>Non-operating revenue</en><fr>Produits hors exploitation</fr><it>Ricavi estranei</it></values>,,/Expense/Betriebsfremder Erfolg
    8500,CHF,<values><de>Ausserordentlicher Aufwand</de><en>Extraordinary expenses</en><fr>Charges exceptionelles</fr><it>Costi straordinari</it></values>,,/Expense/Betriebsfremder Erfolg
    8510,CHF,<values><de>Ausserordentlicher Ertrag</de><en>Extraordinary revenue</en><fr>Produits exceptionelles</fr><it>Ricavi straordinari</it></values>,,/Expense/Betriebsfremder Erfolg
    8900,CHF,<values><de>Direkte Steuern</de><en>Direct taxes</en><fr>Impôts directs</fr><it>Imposte dirette</it></values>,,/Expense/Betriebsfremder Erfolg
    9100,CHF,<values><de>Eröffnungsbilanz</de><en>Opening balance</en><fr>Bilan d'ouverture</fr><it>Bilancio di apertura</it></values>,,/Balance/Eröffnung \\ Abschluss
    9200,CHF,<values><de>Jahresgewinn oder -verlust</de><en>Annual profit or loss</en><fr>Bénefice ou perte de l'exercice</fr><it>Utile annuo o perdita annua</it></values>,,/Balance/Eröffnung \\ Abschluss
    9900,CHF,<values><de>Korrekturen</de><en>Corrections</en><fr>Corrections</fr><it>Correzioni</it></values>,,/Balance/Eröffnung \\ Abschluss
"""

# flake8: enable

SETTINGS = {
    "DEFAULT_SETTINGS": {
        "DEFAULT_OPENING_ACCOUNT_ID": 9100,
        "DEFAULT_INPUT_TAX_ADJUSTMENT_ACCOUNT_ID": 1172,
        "DEFAULT_INVENTORY_ASSET_REVENUE_ACCOUNT_ID": 7900,
        "DEFAULT_INVENTORY_DEPRECIATION_ACCOUNT_ID": 6800,
        "DEFAULT_PROFIT_ALLOCATION_ACCOUNT_ID": 9200,
        "DEFAULT_SALES_TAX_ADJUSTMENT_ACCOUNT_ID": 2202,
        "DEFAULT_INVENTORY_ARTICLE_REVENUE_ACCOUNT_ID": 3200,
        "DEFAULT_INVENTORY_ARTICLE_EXPENSE_ACCOUNT_ID": 4200,
        "DEFAULT_DEBTOR_ACCOUNT_ID": 1100,
        "DEFAULT_INVENTORY_DISPOSAL_ACCOUNT_ID": 6801,
        "DEFAULT_EXCHANGE_DIFF_ACCOUNT_ID": 6960,
        "DEFAULT_CREDITOR_ACCOUNT_ID": 2000
    },
    "reporting_currency": "CHF",
    "DEFAULT_ROUNDINGS":[
        {
            "account": 6961,
            "name": "<values><de>Auf 0.05 runden</de><en>Round to 0.05</en></values>",
            "rounding": 0.05,
            "mode": "HALF_UP",
            "description": None,
            "value": None,
            "referenced": False
        },
        {
            "account": 6961,
            "name": "<values><de>Auf 1.00 runden</de><en>Round to 1.00</en></values>",
            "rounding": 1.0,
            "mode": "HALF_UP",
            "description": None,
            "value": None,
            "referenced": False
        }
    ]
}


def main():
    cashctrl_ledger = CashCtrlLedger()
    tax = pd.read_csv(StringIO(TAX_CODES), skipinitialspace=True)
    accounts = pd.read_csv(StringIO(ACCOUNTS), skipinitialspace=True)
    cashctrl_ledger.restore(
        settings=SETTINGS, tax_codes=tax, accounts=accounts, ledger=pd.DataFrame({})
    )


if __name__ == "__main__":
    main()
