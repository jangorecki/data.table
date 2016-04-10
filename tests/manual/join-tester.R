source("~/Projects/data.table/tests/manual/join-tester-funs.R")

# demo single tests
dt = data.table(id="x", 
                a=as.integer(c(3,8,8,15,15,15,16,22,22,25,25)),
                b=as.integer(c(9,10,25,19,22,25,38,3,9,7,28)),
                c=as.integer(c(22,33,44,14,49,44,40,25,400,52,77)))
set.seed(1L)
dt=dt[sample(.N)]

# which=TRUE
join.sql.equal(dt, dt, which=TRUE, on = c("id==id","a>=a"), verbose=FALSE) # vs sqlite - retain order and check match, has default: allow.cartesian=TRUE
# it warns on database connection closing "In .local(conn, ...) : Closing open result set", it isn't the case for batch processing

# which=FALSE
join.sql.equal(dt, dt, which=FALSE, on = c("id==id","a>=a"), verbose=FALSE)

# which=FALSE, mult="first" - not yet ready for non-equi joins! try equi join below
# join.sql.equal(dt, dt, which=FALSE, mult="first", on = c("id==id","a>=a"), verbose=TRUE)
join.sql.equal(dt, dt, which=FALSE, mult="first", on = c("id==id","a==a"), verbose=TRUE)
join.sql.equal(dt, dt, which=FALSE, mult="last", on = c("id==id","a==a"), verbose=TRUE)
join.sql.equal(dt, dt, which=FALSE, mult="all", on = c("id==id","a==a"), verbose=TRUE)

# which=TRUE, mult="first" - not yet ready for non-equi joins! try equi join below
# join.sql.equal(dt, dt, which=FALSE, mult="first", on = c("id==id","a>=a"), verbose=TRUE)
join.sql.equal(dt, dt, which=TRUE, mult="first", on = c("id==id","a==a"), verbose=TRUE)

# small batch explicitly defined tests
on.v = list(
    c("id==id","a>=a","b>=b"),
    c("id==id","a>=a","b>=b","c>=c")
)
r = batch.join.sql.equal(x = dt, i = dt, on.v = on.v, verbose=TRUE)
all(rapply(r, isTRUE))
str(r) # preview returned structure

# bigger volume
dt = CJ(id="x",
        a=as.integer(c(3,8,8,15,15,15,16,22,22,25,25)),
        b=as.integer(c(9,10,25,19,22,25,38,3,9,7,28)),
        c=as.integer(c(22,33,44,14,49,44,40,25,400,52,77)))
set.seed(1L)
dt=dt[sample(.N)]
r = batch.join.sql.equal(x = dt, i = dt, on.v = on.v, verbose=TRUE)
all(rapply(r, isTRUE))

# populate tests from `populate.on.v`
dt = data.table(id="x",
                a=as.integer(c(3,8,8,15,15,15,16,22,22,25,25)),
                b=as.integer(c(9,10,25,19,22,25,38,3,9,7,28)),
                c=as.integer(c(22,33,44,14,49,44,40,25,400,52,77)))
set.seed(1L)
dt=dt[sample(.N)] 
jn.on = c("id","a","b","c")
on.v = populate.on.v(dt, dt, jn.on, jn.on) # 125 tests
r = batch.join.sql.equal(x = dt, i = dt, on.v = on.v, verbose=TRUE)
all(rapply(r, isTRUE))

# random data: set.seed(unclass(Sys.time()))
set.seed(45L)
dt1 = nq_fun(400L)
dt2 = nq_fun(50L)
x = na.omit(dt1)
y = na.omit(dt2)
jn.on = c(paste0("i",1:4), paste0("d",1:4))
on.v = populate.on.v(x, y, jn.on, jn.on, cap = 2e1) # without cap it would produce 390625 `on` tests! cap default is 1e3, this gets multiplied by number of arg combinations
r = batch.join.sql.equal(x = x, i = y, on.v = on.v, verbose=TRUE)
all(rapply(r, isTRUE))
# get non equal issue
bad = rapply(r, function(x) paste(x, collapse="\n"), "character") 
# print just first one
cat(bad[[1L]])
# re-run single data.table query - add row_id on the flow
addrowid = function(x) data.table:::shallow(x)[, row_id := 1:.N]
addrowid(x)[addrowid(y), 
            .(x.i1, x.i2, x.i3, x.i4, x.d1, x.d2, x.d3, x.d4, x.c1, x.c2, x.row_id, i.i1, i.i2, i.i3, i.i4, i.d1, i.d2, i.d3, i.d4, i.c1, i.c2, i.row_id), 
            nomatch = NA_integer_, 
            mult = "all", 
            which = FALSE, 
            on = c("i4==i4", "d1>d1", "i3<=i3", "i1>=i1", "d3>d3", "d2>d2", "d4>=d4", "i2==i2"), 
            allow.cartesian = TRUE]
