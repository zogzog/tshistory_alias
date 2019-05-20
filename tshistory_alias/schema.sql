create table "{ns}".outliers (
  serie text not null primary key,
  min double precision,
  max double precision
);


create table "{ns}".arithmetic (
  id serial primary key,
  alias text not null,
  serie text not null,
  coefficient double precision,
  fillopt text
);

create index "ix_{ns}_arithmetic_serie" on "{ns}".arithmetic (serie);


create table "{ns}".priority (
  id serial primary key,
  alias text not null,
  serie text not null,
  priority integer not null,
  coefficient double precision default 1,
  prune integer
);

create index "ix_{ns}_priority_alias" on "{ns}".priority (alias);
create index "ix_{ns}_priority_serie" on "{ns}".priority (serie);
