from collections import namedtuple

Dataset = namedtuple("Dataset", ["name", "path", "vcount", "ecount"])

livejournal = Dataset(
    name="LiveJournal",
    path="./data/bin32/shuffled/livejournal/livejournal.bin",
    vcount=4847571,
    ecount=68993773,
)

friendster = Dataset(
    name="Friendster",
    path="./data/bin32/shuffled/friendster/friendster.bin",
    vcount=68349466,
    ecount=2586147869,
)

twitter = Dataset(
    name="Twitter",
    path="./data/bin32/shuffled/twitter_rv/twitter_rv.bin",
    vcount=61578415,
    ecount=1468365182,
)

protein3 = Dataset(
    name="Protein",
    path="./data/bin32/shuffled/protein/protein3.bin",
    vcount=8745542,
    ecount=1058120062,
)

uk2007 = Dataset(
    name="UK2007",
    path="./data/bin32/shuffled/uk2007/uk2007.bin",
    vcount=110123614,
    ecount=3944932566,
)

protein2 = Dataset(
    name="Protein2",
    path="./data/bin32/shuffled/protein2/protein2.bin",
    vcount=17491086,
    ecount=4234728014,
)

DATASETS: list[Dataset] = [
    livejournal,
    # wikipedia,   # XPGraph BFS got wrong result
    protein3,
    twitter,
    friendster,
    uk2007,
    protein2,
    # kron29,       # Too large for 128GB RAM
    # kron30,
]

u_livejournal = Dataset(
    name="LiveJournal",
    path="./data/bin32/undirected/livejournal/livejournal.bin",
    vcount=4847571,
    ecount=42851237,
)

u_protein3 = Dataset(
    name="Protein",
    path="./data/bin32/undirected/protein/protein3.bin",
    vcount=8745542,
    ecount=654620251,
)

u_twitter = Dataset(
    name="Twitter",
    path="./data/bin32/undirected/twitter_rv/twitter_rv.bin",
    vcount=61578415,
    ecount=1202513046,
)

u_friendster = Dataset(
    name="Friendster",
    path="./data/bin32/undirected/friendster/friendster.bin",
    vcount=68349466,
    ecount=1811849342,
)

u_uk2007 = Dataset(
    name="UK2007",
    path="./data/bin32/undirected/uk2007/uk2007.bin",
    vcount=110123614,
    ecount=3448528200,
)

u_protein2 = Dataset(
    name="Protein2",
    path="./data/bin32/undirected/protein2/protein2.bin",
    vcount=17491086,
    ecount=2619978055,
)

U_DATASETS: list[Dataset] = [
    u_livejournal,
    u_protein3,
    u_twitter,
    u_friendster,
    u_uk2007,
    u_protein2,
]

def dataset_by_name(name: str, datasets: list=DATASETS) -> Dataset:
    for d in datasets:
        if d.name == name:
            return d
    return None
