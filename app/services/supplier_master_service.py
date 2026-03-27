"""
Supplier Master Database Service.

Cross-organization knowledge base of supplier → PIANOo category mappings.
Auto-populated from confirmed categorizations, used for suggestions in new orgs.
"""
from __future__ import annotations

import io
import logging
import re
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.category import InkoopCategory
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_master_category import SupplierMasterCategory
from app.services.import_service import normalize_supplier_name

logger = logging.getLogger(__name__)


# ── Keyword rules for AI category suggestion ─────────────────────────
# Each tuple: (regex_pattern, woco_category_nummer, confidence 0-1)
# Order matters: more specific patterns first, generic catch-alls last

CATEGORY_KEYWORD_RULES: list[tuple[str, str, float]] = [
    # === INSTALLATIES ===
    # Elektrotechniek (WC035)
    (r"electr|elektr|elekt|e[\-\.]?tech|elkro", "WC035", 0.85),
    # CV Individueel (WC026)
    (r"cv[\s\-]|verwarming|warmte(?!pomp)|ketel|stook|radiat|thermos|atag\s*nederland|engeltherm|etherma|comfort\s*partners|gasservice", "WC026", 0.85),
    # CV, MV en WTW (WC027)
    (r"wtw|warmteterugwin|excelair|novenco", "WC027", 0.80),
    # MV en WTW (WC077)
    (r"ventilat|luchtbehandel|airco|condair", "WC077", 0.80),
    # Klimaatinstallaties (WC058)
    (r"klimaat|koeling|koeltechn", "WC058", 0.80),
    # Liften (WC065)
    (r"lift(?:en|techniek|service|keur|force|wrap|intermediair|plan|instituut)?(?:\s|$|\.)|elevator|kone\s|otis\s|orona|alimak|l[öo]dige|savaria|tk\s*home\s*solutions", "WC065", 0.90),
    # Liftadviseur (WC064)
    (r"liftadv|lift.*advi|chex\s*lift", "WC064", 0.85),
    # Loodgieter (WC066)
    (r"loodgieter|sanitair|waterleiding|aqua\s*d\s*en\s*s|aquative|bluwaco", "WC066", 0.85),
    # Automatische deuren (WC008)
    (r"automatische?\s*deur|deurautoma|tourniquet|dormakaba|boon\s*edam|tormax|geze|novoferm|faac|holland\s*deuren", "WC008", 0.85),
    # Zonnepanelen (WC118)
    (r"zonne?panel|solar|photovolta|pv[\-\s]?install|pv[\-\s]?control|wocozon|zonne?\s*energ|sungevity|ecorus|norm\s*zon|iwell|audax\s*renewab", "WC118", 0.90),
    # Zonwering (WC119)
    (r"zonwering|markies|screen|rolluik|wessels\s*rolluik|isoluik", "WC119", 0.85),
    # Technische installaties (WC100)
    (r"technische?\s*installat|installatie(?:techniek|bedrijf|groep|service|werken)|installateu|imtech|croonwolter|kropman|breman(?:\s|$)|lomans|van\s*dorp\s*installat|van\s*losser\s*installat|van\s*vliet\s*installat|vink\s*installat|mampaey\s*installat|hartman\s*installat|kemp\s*installat|knol\s*installat|bos\s*installat|schoonderbeek\s*installat", "WC100", 0.75),
    # Werktuigbouwkundige installateurs (WC114)
    (r"werktuigbouw|w[\-\.]?installat|wilo", "WC114", 0.80),
    # WKO (WC116)
    (r"wko|warmte[\-\s]?koude[\-\s]?opslag|bodemenergie|bron[\-\s]?technolog|if\s*technology|eteck", "WC116", 0.85),
    # Hydroforen (WC046)
    (r"hydrofoor|hydroforen|pomp(?:techniek|en)?(?:\s|$|\.)(?!.*brand)|dab\s*pumps|duijvelaar|krijnen\s*pomp", "WC046", 0.80),
    # Intercom (WC049)
    (r"intercom", "WC049", 0.85),
    # Verlichting (WC107)
    (r"verlichting|lichtconstructie|lightronics|citytec", "WC107", 0.80),
    # Kathodische bescherming (WC054)
    (r"kathodisch", "WC054", 0.90),

    # === VEILIGHEID ===
    # Brandbeveiliging - blusmiddelen (WC018)
    (r"brand(?:beveilig|detect|preventie|weer|blus|veilig)|sprinkler|noodverlich|fire[\-\s]?(?:safety|control|job)|saval|ansul|pbt\s*brand|flame\s*control|hoefnagels\s*fire|dr[äa]ger|hertek|naqubo\s*brandwacht", "WC018", 0.85),
    # Brandbeveiliging - BMI (WC019)
    (r"brand[\-\s]?meld|bmi", "WC019", 0.80),
    # Rookmelders (WC095)
    (r"rookmeld", "WC095", 0.90),
    # Beveiliging (WC013)
    (r"beveilig(?:ing|s)?(?:\s|$|\.)(?!.*brand)|security|alarm(?:installat|systeem)|securitas|g4s|voskamp\s*beveilig|een\s*veilig\s*gevoel|westvlietbewak|safety\s*service", "WC013", 0.85),
    # Camerabeveiliging (WC021)
    (r"camera(?:bewak|beveilig)|cctv|viewcontrol", "WC021", 0.85),
    # Toegangscontrole (WC104)
    (r"toegangscontrole|assa\s*abloy|access\s*control|otd\s*toegang", "WC104", 0.80),
    # Meldkamer (WC070)
    (r"meldkamer|alert\s*(?:group|tele)", "WC070", 0.75),
    # Bliksembeveiliging (WC016)
    (r"bliksem", "WC016", 0.90),
    # Valbeveiliging (WC105)
    (r"valbeveiliging|valbescherming|valprotect|daksafe|xsplatform|eurosafe", "WC105", 0.85),
    # Legionella (WC062)
    (r"legionella|water\s*kwaliteit|water\s*behandel|thermisch\s*waterbeheer|helderheid\s*over\s*water|holland\s*water", "WC062", 0.85),
    # Veiligheid generiek (WC106)
    (r"veiligheid(?:sregio)?|kiwa\s*services|t[üu]v\s*nord|aboma|sgs\s*intron|eurofins\s*(?:c[\-\s]?mark|sanitas)|install\s*keur|keurhuis", "WC106", 0.65),
    # NOM Woningen (WC079)
    (r"nom[\-\s]?woning|nul[\-\s]?op[\-\s]?de[\-\s]?meter|energieneutraal", "WC079", 0.80),

    # === VASTGOED ===
    # Schilderwerk (WC096)
    (r"schilder|verf|lakwerk|coating(?:s)?(?:\s|$)|afwerk", "WC096", 0.85),
    # Dakonderhoud (WC031)
    (r"dak(?:onderhoud|bedekking|dekker|techniek|werk|isolatie|systeem|pannen|accent|beheer)|bitum|roofing|dakrand|oranjedak|topdak|vugtdak|groendak|patina\s*dak|total\s*roof", "WC031", 0.85),
    # Dakgoten (WC029)
    (r"dakgoot|dakgoten(?!\s*reiniging)", "WC029", 0.85),
    # Dakgotenreiniging (WC030)
    (r"dakgoten?\s*reinig|gootreiniging", "WC030", 0.80),
    # Glas (WC042)
    (r"glas(?:handel|zetter|bedrijf|service|werk|centrale|groep|bewassing)?(?:\s|$|\.)(?!.*vezel)|beglaz|dubbel\s*glas|isolatie\s*glas|ruitenheer|buijs\s*glas|gsf\s*glas", "WC042", 0.85),
    # Kozijnen (WC059)
    (r"kozijn|raam(?:werk)?|puien|gevelelement|kunststof\s*profiel", "WC059", 0.80),
    # Timmerwerk (WC103)
    (r"timmer(?:man|werk|bedrijf)|hout(?:bouw|handel|bewerking)?(?:\s|$|\.)(?!.*kachel)", "WC103", 0.80),
    # Voegwerk (WC110)
    (r"voeg(?:werk|bedrijf)", "WC110", 0.85),
    # Gevelonderhoud (WC041)
    (r"gevel(?:onderhoud|reiniging|renovatie|beheer|werken|elan)?(?:\s|$|\.)", "WC041", 0.80),
    # Beton / Straatwerk (WC012)
    (r"beton(?:werk|reparatie|renovatie|techniek)?(?:\s|$|\.)|betonherstel", "WC012", 0.80),
    # Straatwerk (WC099)
    (r"straat(?:werk|maker)|bestrat(?:ing)?|tegel(?:s|werk|zetter)?(?:\s|$|\.)|klinker", "WC099", 0.80),
    # Isolatie (WC050)
    (r"isolat(?:ie|or|uik)?(?:\s|$|\.)(?!.*glas)|spouwmuur|purschuim|na[\-\s]?isolat", "WC050", 0.80),
    # Riolering (WC094)
    (r"riol(?:ering)?|riool|ontstop|afvoer(?:techniek)?|drainage", "WC094", 0.80),
    # Hang en sluitwerk (WC044)
    (r"hang[\-\s]?(?:en|&)[\-\s]?sluit|sloten|slotenmaker|sluitwerk|siersmederij|hekwerk|heras\s", "WC044", 0.80),
    # Sloopwerken (WC098)
    (r"sloop(?:werk|bedrijf|techniek)?(?:\s|$|\.)|demontage|kuyer|vlasman\s*beton", "WC098", 0.85),
    # Asbest (WC007)
    (r"asbest|sanering(?!\s*(?:bodem|grond))", "WC007", 0.85),
    # Milieu/Verontreiniging aannemer (WC073)
    (r"milieu[\-\s]?(?:sanering|verontreiniging|techniek|beheer)|bodem(?:sanering|onderzoek)|steenbruggen\s*milieu|van\s*dijk\s*geo|vanderhelm\s*milieu", "WC073", 0.80),
    # Duurzaamheid aannemer (WC034)
    (r"duurzaam(?:heid)?(?:\s|$|\.)\s*(?:aannemer|bouw)", "WC034", 0.75),
    # PO aannemer (WC088)
    (r"planmatig[\-\s]?onderhoud|po[\-\s]?aannemer", "WC088", 0.75),
    # Dagelijks onderhoud (WC028)
    (r"dagelijks[\-\s]?onderhoud|mutatiedienst|mutatie[\-\s]?onderhoud", "WC028", 0.75),
    # Materiaal (WC068)
    (r"bouwmateria|bouwmarkt|ijzerwaren|gereedschap|technische\s*unie|isero|neijenesch|davo\s*doe\s*het|horsman|wasco\s*groothandel|handelsondernemi|polvo\s|de\s*lange\s*ijzer", "WC068", 0.75),
    # Vloeren
    (r"vloer(?:en|leg|verwarming)?|parket|laminaat|pvc[\-\s]?vloer|vinyl|projectvloer|vloerveilig", "WC099", 0.75),

    # === FACILITAIR ===
    # Schoonmaak (WC097)
    (r"schoonma[a]?k|schoonhoud|reinig(?:ing|s)?(?:techniek)?(?:\s|$|\.)(?!.*riool|.*dakgoot)|clean(?:ing|fellows|ers)?(?:\s|$|\.)|glazenwas|gom\s|csu\s|centaur\s*clean|rein\s*en\s*fijn|b2[\-\s]?clean|all\s*fresh|mako\s*clean|de\s*zwart\s*facilitair|arco\s*reinig|bijvelds\s*reinig|fonville", "WC097", 0.85),
    # Groenonderhoud (WC043)
    (r"groen(?:onderhoud|voorziening|beheer|service|ladder|oord)?(?:\s|$|\.)|hoveniersbedrijf|hovenier|tuin(?:onderhoud|aanleg|service|en\s|totaal)?(?:\s|$|\.)|plantsoen|boom(?:verzorg|ingenieur|specialisten|chirurg)|krinkels|idverde|copijn|sproeibedrijf|ploegmakers\s*cultuur|bruinsma\s*hydro|hoogendoorn\s*project|cathelijne\s*vreeburg|ambius|de\s*witte\s*tuin|greenlife|terpstra\s*tuin|verheij\s*groen|j[\.\s]*van\s*huizen\s*groen", "WC043", 0.85),
    # Ongediertebestrijding (WC004 - using Afval as closest)
    (r"ongedierte|plaagdier|anticimex|rentokil|vermin\s*fighter|dutch\s*bird|normec\s*biobeheer|o[\.\s]*b[\.\s]*m[\.\s]*ongedierte", "WC004", 0.75),
    # Afval (WC004)
    (r"afval(?:verwerk|inzamel|beheer)?(?:\s|$|\.)|recycl|renewi|irado|container(?:dienst|verhuur)?(?:\s|$|\.)(?!.*bouw)|hartog\s*container", "WC004", 0.80),
    # Catering (WC022)
    (r"catering|koffie(?:click)?|automaat|vending|kantine|nespresso|douwe\s*egberts|jacobs|fruitful|gio\s*coffee|sligro|metro\s*cash|qlichef|yellow\s*kitchen|viteau|culligan|animo\s", "WC022", 0.80),
    # Kleding (WC057)
    (r"bedrijfskleding|werkkleding|pbm|persoonlijk\s*bescherm|kleding", "WC057", 0.80),
    # Meubilair (WC071)
    (r"meubilair|meubel|kantoor(?:inricht|meubilair)|interieur(?:bouw)?(?:\s|$|\.)|nijha|stoffering", "WC071", 0.75),
    # Kantoor en werkplaatsinrichting (WC052)
    (r"kantoor\s*(?:en\s*)?werkplaats|werkplaats\s*inricht", "WC052", 0.75),
    # Kantoorartikelen (WC053)
    (r"kantoorartik|kantoorbenodigdh|office\s*supplies|discount\s*office", "WC053", 0.80),
    # Geschenken (WC040)
    (r"geschenk|cadeau|cadeaubon|greetz|topgeschenk|snoepwinkel|bloemen|oudshoorn\s*bloem|faircasso", "WC040", 0.80),
    # Wagenpark (WC113)
    (r"wagenpark|autolease|lease(?:auto)?(?:\s|$|\.)|multilease|fleet|mobiliteit|bike[\-\s]?(?:motion|mob)|fietsen|auto\s*versteeg|broekhuis|vakgarage|van\s*mossel|santander\s*leas|on[\-\s]?route|ecarwrapper|ecotap|shell\s*ev\s*charg", "WC113", 0.75),
    # Mobiliteit (WC075)
    (r"mobiliteit|parkeer|smsparking|p1\s*on", "WC075", 0.70),
    # Verhuizing / Transport (WC109)
    (r"verhuis|verhuiz|transport(?:bedrijf|dienst)?(?:\s|$|\.)", "WC109", 0.80),
    # Post (WC089)
    (r"post(?:verwerk|nl|bezorg|dienst|ex)?(?:\s|$|\.)|kopieer|druk(?:werk|kerij)|print(?:er|service|werk|ing)?(?:\s|$|\.)|repro|canon|konica|quadient", "WC089", 0.75),
    # Gebouwbeheer (WC039)
    (r"gebouwbeheer|facilitair(?:e)?\s*(?:dienst|service)?(?:\s|$|\.)|facility\s*service|facilicom|vebego|aniel\s*facilitair|lever\s*facilitair|branderhorst\s*facilitair|blue\s*facilitair|tomin\s*facilitair|cws\s*hygiene", "WC039", 0.70),
    # Meetapparatuur (WC069)
    (r"meetapparat|meettechn|joulz\s*meetbedrijf|kenter", "WC069", 0.75),

    # === ICT ===
    # ICT (WC047)
    (r"ict[\s\-]|software|automat(?:isering)?(?:\s|$|\.)|comput|digita[al]|cloud|saas|it[\-\s]?(?:dienst|building)|system(?:s|en)?(?:\s|$|\.)(?!.*veiligheid)|esri|accensys|apptimize|alphaplan|ibabs|visionplanner|vitec\s*vabi|docufiller|itris|webeasy|findwhere|de\s*ictivity|stip[\-\s]?connected|atlantis\s*network", "WC047", 0.75),
    # CLM software (WC023)
    (r"clm|contract\s*(?:management|lifecycle)|spacewell|planon", "WC023", 0.80),
    # Telecom (WC101)
    (r"telecom|kpn|vodafone|ziggo|odido|t[\-\s]?mobile|wireless\s*logic", "WC101", 0.80),
    # Telefonie (WC102)
    (r"telefon|teleservice", "WC102", 0.75),
    # AV middelen (WC009)
    (r"av[\-\s]?middel|audio[\-\s]?visu|beamer|projectie|audivio", "WC009", 0.75),

    # === ADVIES ===
    # Architecten (WC006)
    (r"architect", "WC006", 0.90),
    # Notaris (WC080)
    (r"notaris", "WC080", 0.90),
    # Juridisch adviseur (WC051)
    (r"juridisch|advocat|recht(?:s)?(?:\s|$|\.)|gerechtsdeurwaarder|dirkzwager", "WC051", 0.80),
    # Accountant (WC002)
    (r"account(?:ant|ancy)|boekhou|belasting(?:advies|consult)", "WC002", 0.85),
    # Makelaar (WC067)
    (r"makelaar|makelaardij|taxat(?:ie|eur)|waardebepaling", "WC067", 0.85),
    # Bouwadvies (WC017)
    (r"bouw(?:advies|management|begeleiding|toezicht|kund)|projectmanag|bouwend\s*nederland|innax|bremen\s*bouwadv|nebest|wsp\s*nederland|stantec|calculatiebur", "WC017", 0.75),
    # Energielabels (WC037)
    (r"energie\s*label|energieprestatie|epa[\-\s]?advie|ep[\-\s]?advie", "WC037", 0.80),
    # Energie (WC036)
    (r"energi[eë]|nutsbedrijf|stedin|eneco|essent|vattenfall|stroom|adam\s*energy|energy|eteck", "WC036", 0.70),
    # Conditiemeting (WC024)
    (r"conditiemeting|nen\s*2767|onderhoud(?:s)?kwaliteit", "WC024", 0.80),
    # Financieel adviseur (WC038)
    (r"financ(?:ieel|iering|ial)?(?:\s|$|\.)|ortec\s*finance|bnp\s*paribas", "WC038", 0.70),
    # Milieu advies (WC072)
    (r"milieu(?:\s*advies)?(?:\s|$|\.)|duurzaamheid\s*advies", "WC072", 0.70),
    # Plattegronden (WC087)
    (r"plattegrond|inmeten|rappr", "WC087", 0.75),
    # Prijzenboek (WC091)
    (r"prijzenboek", "WC091", 0.90),
    # Constructie advies (WC083 - Overige adviesdiensten)
    (r"construct|statiek|ingenieur(?:s)?(?:buro|bureau)?(?:\s|$|\.)|raadgevend|nieman|de\s*blaay", "WC083", 0.65),
    # BKT (WC015)
    (r"bkt|bouwkost", "WC015", 0.80),
    # Advies generiek (WC083 - Overige adviesdiensten)
    (r"advies(?:bureau|buro|groep)?(?:\s|$|\.)|consultanc|consult(?:ing|ants|s)?(?:\s|$|\.)|accent\s*adviseur|kp\s*adviseur|rho\s*adviseur|vgg\s*adviseur", "WC083", 0.55),

    # === PERSONEEL ===
    # Inleen- en uitzendkrachten (WC048)
    (r"uitzend|deta(?:chering|cheren)|werving|recruitment|personeels|interim|flex(?:werk)?(?:\s|$|\.)|payroll", "WC048", 0.80),
    # Werving & selectie (WC115)
    (r"werving|selectie", "WC115", 0.75),
    # Arbo (WC005)
    (r"arbo(?:dienst|advies)?(?:\s|$|\.)|arbeid(?:s)?(?:omstandigh|veiligheid)|procardio|aed", "WC005", 0.80),
    # Opleiding (WC081)
    (r"opleid(?:ing|en)|training|cursus|academie|scholing|e[\-\s]?learning", "WC081", 0.80),

    # === COMMUNICATIE ===
    # PR/marketing/communicatie (WC090)
    (r"communicat|reclame|marketing|media(?:\s|$|\.)|creatief|design|ontwerp(?:\s|$|\.)|grafisch|webdesign|website|reclame|advertent|signmaker|letterplex|rolette|graphic", "WC090", 0.70),
    # Huisstijl (WC045)
    (r"huisstijl", "WC045", 0.85),

    # === SOCIAAL ===
    # Bewonersbegeleiding (WC014)
    (r"bewoner|woonbegeleiding|huurder", "WC014", 0.70),
    # Klanttevredenheidsmeting (WC056)
    (r"klant(?:tevredenheid|onderzoek)|kto|sensus[\-\s]?methode", "WC056", 0.80),
    # Ketenpartners (WC055)
    (r"maatschappelijk|sociaal(?:\s|$|\.)|hulpverlening|welzijn|zorg(?:groep)?(?:\s|$|\.)|stichting\s*(?:leviaan|timon|minters|profila|kwadraad|enver)", "WC055", 0.60),
    # Leefbaarheid (WC060)
    (r"leefbaarheid|burenlawaai|overlast", "WC060", 0.75),
    # Calamiteitenpartners (WC020)
    (r"calamiteit|first\s*response|waterschade|emergency", "WC020", 0.75),

    # === FINANCIEEL ===
    # Verzekering (WC108)
    (r"verzeker|assurant|acrisure|centraal\s*beheer|all\s*risk", "WC108", 0.80),
    # Abonnementen (WC001)
    (r"abonnement|lidmaatschap|vakmedia|financ(?:ieel|iële)?\s*dagblad|dun\s*&\s*bradstreet|creditsafe|graydon", "WC001", 0.70),

    # === VASTGOEDBEHEER ===
    # Leegstandsbeheer (WC061)
    (r"leegstand|leegstands|villex|anti[\-\s]?kraak", "WC061", 0.80),
    # VVE beheer (WC111)
    (r"vve[\-\s]?beheer", "WC111", 0.85),
    # Projectontwikkelaar (WC092)
    (r"project\s*ontwikkel|bpd\s*ontwikkel", "WC092", 0.75),

    # === VASTGOEDBEHEER ===
    # Mutatie onderhoud (WC076)
    (r"mutat(?:ie|ies)|woningontruim|moving[\-\s]?in|multitect", "WC076", 0.75),
    # VVE (WC112)
    (r"vve|vereniging\s*van\s*eigenar", "WC112", 0.75),
    # Leegstandsbeheer - also Woningnet
    (r"woningnet", "WC085", 0.60),

    # === GENERIEKE CATCH-ALL ===
    # Techniek generiek (WC100) - for "Techniek" and "Technisch" companies
    (r"techniek|technisch(?:\s*(?:beheer|bedrijf|buro))?(?:\s|$|\.)|technieken", "WC100", 0.50),
    # Service/montage bedrijven → Overige aannemer (WC082)
    (r"montagebedrijf|service(?:\s*bedrijf)?(?:\s|$|\.)", "WC082", 0.45),
    # Overige aannemer (WC082) - for general construction companies
    (r"aannemer|aanneming|bouw(?:bedrijf|onderneming|groep)?(?:\s|$|\.)(?!.*(?:advies|management|toezicht|kund|materiaal))|renovat|restaurat|onderhoud(?:s)?(?:bedrijf|dienst)?(?:\s|$|\.)|woningverbeter|woningbeheer|heilijgers|hemubo|breijer|donker\s*groep|janssen\s*de\s*jong|coen\s*hagedoorn|van\s*wijnen|bam\s*groep|kondor\s*wessels|verkerk\s*groep|hermans\s*groep|de\s*werf\s*groep", "WC082", 0.60),
    # Overige bedrijfskosten (WC084) - catch-all for businesses
    (r"bol\.com|siemens|boels\s*verhuur|bredenoord|jungheinrich|canon|konica\s*minolta|hobart|bp\s*europa", "WC084", 0.40),
]


def suggest_category_for_name(name: str, categories_by_nummer: dict[str, dict] | None = None) -> dict | None:
    """
    Suggest a WoCo category for a supplier name using keyword rules.
    Returns: {"category_nummer": "WCxxx", "category_naam": "...", "confidence": 0.xx} or None
    """
    name_lower = name.lower().strip()
    for pattern, cat_nummer, confidence in CATEGORY_KEYWORD_RULES:
        if re.search(pattern, name_lower):
            result = {
                "category_nummer": cat_nummer,
                "confidence": confidence,
            }
            if categories_by_nummer and cat_nummer in categories_by_nummer:
                result["category_naam"] = categories_by_nummer[cat_nummer]["inkooppakket"]
                result["category_id"] = categories_by_nummer[cat_nummer].get("id")
            return result
    return None


class SupplierMasterService:
    def __init__(self, db: Session):
        self.db = db

    # ── Core CRUD ───────────────────────────────────────────────────

    def upsert(
        self,
        normalized_name: str,
        display_name: str,
        category_id: int,
        category_nummer: str,
        category_name: str,
        source: str = "auto",
        category_system: str = "woco",
    ) -> SupplierMasterCategory:
        """Add or update a supplier-category mapping. Increments usage_count if exists."""
        existing = (
            self.db.query(SupplierMasterCategory)
            .filter(
                SupplierMasterCategory.normalized_name == normalized_name,
                SupplierMasterCategory.category_id == category_id,
                SupplierMasterCategory.category_system == category_system,
            )
            .first()
        )
        if existing:
            existing.usage_count += 1
            existing.updated_at = datetime.utcnow()
            if source == "manual":
                existing.source = "manual"
            return existing
        else:
            entry = SupplierMasterCategory(
                normalized_name=normalized_name,
                display_name=display_name,
                category_id=category_id,
                category_nummer=category_nummer,
                category_name=category_name,
                category_system=category_system,
                usage_count=1,
                source=source,
            )
            self.db.add(entry)
            return entry

    def record_categorization(
        self, supplier: Supplier, categorization: SupplierCategorization
    ) -> None:
        """Called after a categorization is confirmed. Upserts into master DB."""
        if categorization.source not in (
            "manual", "ai_accepted", "ai_confirmed", "imported",
        ):
            return

        category = self.db.query(InkoopCategory).get(
            categorization.category_id
        )
        if not category:
            return

        self.upsert(
            normalized_name=supplier.normalized_name,
            display_name=supplier.name,
            category_id=category.id,
            category_nummer=category.nummer,
            category_name=category.inkooppakket,
            source="auto",
        )

    # ── Lookup ──────────────────────────────────────────────────────

    def lookup(
        self, normalized_name: str, category_system: str | None = None
    ) -> list[SupplierMasterCategory]:
        """Find all category mappings for a normalized supplier name."""
        q = self.db.query(SupplierMasterCategory).filter(
            SupplierMasterCategory.normalized_name == normalized_name,
        )
        if category_system:
            q = q.filter(SupplierMasterCategory.category_system == category_system)
        return q.order_by(SupplierMasterCategory.usage_count.desc()).all()

    def bulk_lookup(
        self, normalized_names: list[str], category_system: str | None = None
    ) -> dict[str, list[SupplierMasterCategory]]:
        """Lookup multiple supplier names at once (efficient IN-clause)."""
        if not normalized_names:
            return {}
        q = self.db.query(SupplierMasterCategory).filter(
            SupplierMasterCategory.normalized_name.in_(normalized_names),
        )
        if category_system:
            q = q.filter(SupplierMasterCategory.category_system == category_system)
        entries = q.order_by(SupplierMasterCategory.usage_count.desc()).all()
        result: dict[str, list[SupplierMasterCategory]] = {}
        for e in entries:
            result.setdefault(e.normalized_name, []).append(e)
        return result

    # ── Search & List ───────────────────────────────────────────────

    def search(
        self,
        query: str = "",
        page: int = 1,
        page_size: int = 50,
        category_system: str | None = None,
    ) -> tuple[list[SupplierMasterCategory], int]:
        """Search master DB by supplier name or category name."""
        q = self.db.query(SupplierMasterCategory)
        if category_system:
            q = q.filter(SupplierMasterCategory.category_system == category_system)
        if query:
            pattern = f"%{query}%"
            q = q.filter(
                SupplierMasterCategory.display_name.ilike(pattern)
                | SupplierMasterCategory.category_name.ilike(pattern)
                | SupplierMasterCategory.category_nummer.ilike(pattern)
            )
        total = q.count()
        offset = (page - 1) * page_size
        entries = (
            q.order_by(
                SupplierMasterCategory.display_name,
                SupplierMasterCategory.category_nummer,
            )
            .offset(offset)
            .limit(page_size)
            .all()
        )
        return entries, total

    def get_stats(self, category_system: str | None = None) -> dict:
        """Get aggregate statistics for the master DB."""
        base = self.db.query(SupplierMasterCategory)
        if category_system:
            base = base.filter(SupplierMasterCategory.category_system == category_system)

        total_entries = base.count()
        unique_suppliers = (
            base.with_entities(
                func.count(func.distinct(SupplierMasterCategory.normalized_name))
            ).scalar()
            or 0
        )
        top_q = (
            base.with_entities(
                SupplierMasterCategory.category_name,
                func.count(SupplierMasterCategory.id).label("cnt"),
            )
            .group_by(SupplierMasterCategory.category_name)
            .order_by(func.count(SupplierMasterCategory.id).desc())
        )
        top_category = top_q.first()
        return {
            "total_entries": total_entries,
            "unique_suppliers": unique_suppliers,
            "top_category": top_category[0] if top_category else None,
            "top_category_count": top_category[1] if top_category else 0,
        }

    # ── Update & Delete ─────────────────────────────────────────────

    def update_entry(
        self,
        entry_id: int,
        category_id: Optional[int] = None,
        notes: Optional[str] = None,
        display_name: Optional[str] = None,
    ) -> Optional[SupplierMasterCategory]:
        """Update an existing master entry."""
        entry = self.db.query(SupplierMasterCategory).get(entry_id)
        if not entry:
            return None

        if category_id is not None and category_id != entry.category_id:
            category = self.db.query(InkoopCategory).get(category_id)
            if category:
                entry.category_id = category.id
                entry.category_nummer = category.nummer
                entry.category_name = category.inkooppakket

        if notes is not None:
            entry.notes = notes
        if display_name is not None:
            entry.display_name = display_name
            entry.normalized_name = normalize_supplier_name(display_name)

        entry.updated_at = datetime.utcnow()
        return entry

    def delete_entry(self, entry_id: int) -> bool:
        """Delete a single master entry."""
        entry = self.db.query(SupplierMasterCategory).get(entry_id)
        if not entry:
            return False
        self.db.delete(entry)
        return True

    # ── CSV Import ──────────────────────────────────────────────────

    def bulk_upsert_from_csv(
        self, file_bytes: bytes, category_system: str = "woco"
    ) -> dict:
        """
        Process a CSV with supplier_name and category_nummer columns.
        Returns stats: {created, updated, skipped, errors}.
        """
        try:
            df = pd.read_csv(io.BytesIO(file_bytes))
        except Exception as e:
            return {"created": 0, "updated": 0, "skipped": 0, "errors": [str(e)]}

        # Find columns (case-insensitive)
        col_map: dict[str, str] = {}
        for col in df.columns:
            lower = col.strip().lower()
            if "supplier" in lower or "leverancier" in lower or "naam" in lower:
                col_map["name"] = col
            elif "nummer" in lower or "number" in lower or "code" in lower:
                col_map["nummer"] = col
            elif "notes" in lower or "notities" in lower or "opmerking" in lower:
                col_map["notes"] = col

        if "name" not in col_map or "nummer" not in col_map:
            return {
                "created": 0,
                "updated": 0,
                "skipped": 0,
                "errors": [
                    "CSV moet kolommen bevatten voor leveranciersnaam "
                    "(supplier_name/leverancier) en categorienummer "
                    "(category_nummer/nummer)."
                ],
            }

        # Build InkoopCategory lookup by nummer
        cat_q = self.db.query(InkoopCategory)
        if category_system:
            cat_q = cat_q.filter(InkoopCategory.category_system == category_system)
        categories = cat_q.all()
        cat_by_nummer = {c.nummer.strip(): c for c in categories}

        created = 0
        updated = 0
        skipped = 0
        errors: list[str] = []

        for idx, row in df.iterrows():
            name_raw = str(row[col_map["name"]]).strip()
            nummer_raw = str(row[col_map["nummer"]]).strip()
            notes_raw = str(row.get(col_map.get("notes", ""), "")).strip()
            notes_val = notes_raw if notes_raw and notes_raw != "nan" else None

            if not name_raw or name_raw == "nan" or not nummer_raw or nummer_raw == "nan":
                skipped += 1
                continue

            category = cat_by_nummer.get(nummer_raw)
            if not category:
                errors.append(f"Rij {idx + 2}: categorie '{nummer_raw}' niet gevonden")
                continue

            normalized = normalize_supplier_name(name_raw)
            existing = (
                self.db.query(SupplierMasterCategory)
                .filter(
                    SupplierMasterCategory.normalized_name == normalized,
                    SupplierMasterCategory.category_id == category.id,
                    SupplierMasterCategory.category_system == category_system,
                )
                .first()
            )

            if existing:
                existing.usage_count += 1
                existing.updated_at = datetime.utcnow()
                if notes_val:
                    existing.notes = notes_val
                updated += 1
            else:
                entry = SupplierMasterCategory(
                    normalized_name=normalized,
                    display_name=name_raw,
                    category_id=category.id,
                    category_nummer=category.nummer,
                    category_name=category.inkooppakket,
                    category_system=category_system,
                    usage_count=1,
                    source="imported",
                    notes=notes_val,
                )
                self.db.add(entry)
                created += 1

        self.db.commit()
        return {
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "errors": errors,
        }
