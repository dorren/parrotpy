from parrotpy import Parrot
from parrotpy import functions as PF
parrot = Parrot(seed=123)
builder = parrot.df_builder().build_column('id', 'int', PF.auto_increment(start=10000, step=3)).build_column('name', 'str', PF.name()).build_column('address', 'str', PF.address()).build_column('birth_year', 'int', PF.stats.uniform(min=1925, max=2025)).build_column('salary', 'int', PF.stats.normal(mean=70000, std_dev=10000))
df = builder.generate(n=100)