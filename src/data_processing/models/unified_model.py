"Unified Model Module"
from __future__ import annotations

from typing import Union

from pydantic import BaseModel, Field, model_validator

OBJECT_ID_PATTERN = r"^<(IrIf|IR|CSCommsReader|RazorLinkCtx|" \
                    r"RazorLinkCtxMgr|IscCtx|MbcpDefCC|IscBrr|" \
                    r"IscChnDgrm|IscCtxRemIf|SlaMtr)![0-9]+>|" \
                    r"^<IscSchedGrp![0-9]+>\[[a-z0-9]*]"


class Cl(BaseModel):
    "Cl Model"
    ownr: str
    oid: str
    info: str
    sinf: str
    goid: str


class Hd(BaseModel):
    "Header1 Model"
    Ct: str
    Ti: str
    Cl: Cl


class T(BaseModel):
    "T Model"
    nP: str
    nB: str


class Tx3(BaseModel):
    "Tx3 model"
    t: T


class Tk(BaseModel):
    """Tk Model. Here the nTk and nlTk counters
    show if the routing functionality of the vnf is impacted"""

    nTk: int
    nlTk: int
    tlTkT: int


class G1(BaseModel):
    "G1 Model"
    Tk: Tk


class Ctx(BaseModel):
    "Ctx Model"
    ctxId: str
    oid: str


class AI(BaseModel):
    "AI Model"
    sP: int
    sB: int
    fP: int
    fB: int
    tP: int
    tB: int


class Sts3(BaseModel):
    "Sts3 Model"
    aI: AI


class Sts2(BaseModel):
    "Sts2 Model"
    tx: Tx3


class Sts1(BaseModel):
    "Sts1 Model"
    g: G1


class Sta8(BaseModel):
    "Sta8 Model"
    s: int
    v: int


class Sta12(BaseModel):
    id: str


class StaSlaMtrOld(BaseModel):
    g: Sta12


class Sta13(BaseModel):
    "Sta Model for phase 4 sla meter"
    id: str


class BlFrCf(BaseModel):
    "BlFrCf Model"
    nBl: int
    nBy: int
    totRtt: int
    refRtt: int


class BlPo(BaseModel):
    "BlPo Model"
    nBl: int
    nBy: int
    totRtt: int
    refRtt: int


class User(BaseModel):
    "User Model"
    nBlIn: int
    nByIn: int
    BlPo: BlPo
    nBlTo: int
    nByTo: int
    nBlOf: int
    nByOf: int
    nBlDc: int
    nByDc: int
    BlFrCf: BlFrCf


class Stor(BaseModel):
    "Stor Model"
    User: User


class QoSM(BaseModel):
    "QoSM model"
    Stor: Stor


class Sts4(BaseModel):
    "Sts4 Model"
    min: int
    max: int
    ns: int


class Rtt(BaseModel):
    "Rtt Model"
    Sta: Sta8
    Sts: Sts4


class Sta7(BaseModel):
    "Sta7 Model"
    rtt: Rtt


class Sta6(BaseModel):
    "Sta6 Model"
    nIf: int


class Tot(BaseModel):
    t: int
    u: int
    ur: int
    uro: int


class Sts8(BaseModel):
    "Sts Model for sla meter phase 4"
    tot: Tot
    QoSM: QoSM


class Sts(BaseModel):
    "Sts Model for sla meter pre phase 4"
    tot: Tot


class StsSlaMtrOld(BaseModel):
    tx: Sts


class Tx4(BaseModel):
    "Tx4 Model for sla meter phase4 format"
    tx: Sts8


class G(BaseModel):
    "G Model for sla Meter phase4 format"
    g: Sta13


class Val9(BaseModel):
    Sta: G
    Sts: Tx4


class Val8(BaseModel):
    "Sla Meter old Val Model"
    Sta: StaSlaMtrOld
    Sts: StsSlaMtrOld

    @model_validator(mode="before")
    @classmethod
    def generate_node_data(cls, values):
        "Validator that adds tx and g elements"
        if values:
            if ("Sta" in values and "cSl" in values["Sta"] and
                    "Sts" in values and "tot" in values["Sts"]):
                # if SlaMtr Old object add g and tx elements

                nValues = {"Sta": {"g": {}}, "Sts": {"tx": {}}}
                nValues["Sta"]["g"] = values["Sta"]
                nValues["Sts"]["tx"] = values["Sts"]
                return nValues
        return values


class Sta4(BaseModel):
    """Sta4 Model for interface state. The
    pdb element contains in xml format the bitrate
    counters for tx and rx. The netinfo element
    contains the interface state
    """
    IrName: str
    Name: str
    pdb: str
    netinfo: str


class Brrs(BaseModel):
    "Brrs Model"
    nBrrs: int


class G6(BaseModel):
    "G6 Model"
    brrs: Brrs


class Tx10(BaseModel):
    "Tx for Isc Sched Grp model"
    QoSM: QoSM


class Sta2(BaseModel):
    """Sta2 Model for ctx state and age
    state and age are in int format """
    state: str
    statestr: str
    born: str
    age: int


class Sta1(BaseModel):
    "Sta1 Model for number of contexts"
    nCtx: int


class Sta11(BaseModel):
    "Sta11 Model"
    g: G6


class G4(BaseModel):
    "G4 Model"
    CtxId: str
    IfId: str
    IscIfRel: str


class Sta9(BaseModel):
    "Sta9 Model"
    g: G4


class Dr(BaseModel):
    "Dr Model"
    m: int
    t: int
    l: int
    i: int
    u: int


class I2(BaseModel):
    "I2 Model"
    dr: Dr


class MBrr3(BaseModel):
    "Mbrr3 Model"
    I: I2


class Tx5(BaseModel):
    mBrr: MBrr3


class Dr1(BaseModel):
    "Dr1 Model"
    l: int
    o: int
    d: int
    r: int


class I3(BaseModel):
    "I3 Model"
    dr: Dr1


class MBrr4(BaseModel):
    "Mbrr4 Model"
    I: I3


class Rx1(BaseModel):
    "Rx1 Model"
    mBrr: MBrr4


class Ch(BaseModel):
    "Ch Model"
    Id: str
    hC: int
    hD: int


class G7(BaseModel):
    "G Model for Isc Sched Grp"
    id: str


class G5(BaseModel):
    "G5 Model"
    DSched: str


class Sta10(BaseModel):
    "Sta10 Model"
    g: G5


class Sta14(BaseModel):
    "Sta Model for IscSchedGrp"
    g: G7


class Sts5(BaseModel):
    "Sts5 Model"
    tx: Tx5
    rx: Rx1


class Sts9(BaseModel):
    "Sts Model for Isc Sched Grp"
    tx: Tx10


class Val10(BaseModel):
    "Val Model for Isc Sched Grp"
    Sta: Sta14
    Sts: Sts9


class Val6(BaseModel):
    "Val6 Model"
    Sta: Sta10


class Val5(BaseModel):
    "Val5 Model"
    Sta: Sta9
    Sts: Sts5


class Val4(BaseModel):
    "Val4 Model"
    Sta: Sta7
    Sts: Sts3


class Val3(BaseModel):
    "Val3 Model"
    Sts: Sts2


class Val2(BaseModel):
    "Val2 Model"
    Sts: Sts1


class Val1(BaseModel):
    "Val1 Model"
    Sta: Union[Sta1, Sta2, Sta4, Sta6]


class Val7(BaseModel):
    "Val7 Model"
    Sta: Sta11


class Snapshot(BaseModel):
    "Snapshot Model"
    sid: int
    ctx: Union[str, Ctx]
    obj: str = Field(pattern=OBJECT_ID_PATTERN)
    val: Union[Val1, Val2, Val3, Val4, Val5, Val6, Val7, Val9, Val8, Val10]


class UnifiedModel(BaseModel):
    "Razor Link Ctx Model"
    Hd: Hd
    field_0: str = Field(..., alias='0')
    snapshot: Snapshot
