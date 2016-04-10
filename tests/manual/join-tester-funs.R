library(data.table)
if (!"ops" %chin% names(formals(data.table:::bmerge))) stop("this script should be only run for latest devel which should have 'ops' arg in 'data.table:::bmerge'")

# on same as data.table `on` but does not support table prefixes `x.` or `i.`, fields of `x` must be on LHS, and from `i` on RHS
# verbose print processing details with timing, etc.
# .conn SQLite connection, if provided it will use it instead of spawning new one
# .drop logical TRUE (default) will drop db tables before and after and populate new, when FALSE it expects tables to be populated
join.sql.equal = function(x, i, nomatch=NA_integer_, mult="all", which=TRUE, allow.cartesian=TRUE, on, verbose=FALSE, .conn, .drop=TRUE) {
    if (!requireNamespace("DBI", quietly=TRUE)) stop("nonequijoin.all.equal uses DBI to validate results of non equi join against with")
    if (!requireNamespace("RSQLite", quietly=TRUE)) stop("nonequijoin.all.equal uses RSQLite to validate results of non equi join against with")
    if (requireNamespace("microbenchmarkCore", quietly=TRUE)) {
        # install.packages("microbenchmarkCore", repos="https://olafmersmann.github.io/drat")
        system.time = microbenchmarkCore::system.nanotime # drop-in replacement for 'elapsed' timing
    }
    stopifnot(is.data.table(x), is.data.table(i), is.character(mult), is.logical(which), is.logical(allow.cartesian), is.logical(verbose), is.logical(.drop), length(mult)==1L, length(which)==1L)
    # row_id column required as SQL is not ordered, creating on R side
    if (!"row_id" %in% names(x)) x = data.table:::shallow(x)[, row_id := 1:.N]
    if (!"row_id" %in% names(i)) i = data.table:::shallow(i)[, row_id := 1:.N]
    # preparing sql environment
    conn = if (new.conn <- missing(.conn)) DBI::dbConnect(RSQLite::SQLite()) else .conn
    if (.drop) {
        try(DBI::dbSendQuery(conn, "DROP TABLE x;"), silent = TRUE)
        try(DBI::dbSendQuery(conn, "DROP TABLE i;"), silent = TRUE)
        DBI::dbWriteTable(conn, name = "x", value = x)
        DBI::dbWriteTable(conn, name = "i", value = i)
    }
    # building data.table query
    jj = if (!which) as.call(c(
        list(as.name(".")),
        lapply(names(x), function(x.col) as.name(paste0("x.", x.col))),
        lapply(names(i), function(i.col) as.name(paste0("i.", i.col)))
    ))
    dtq = as.call(c(
        list(as.name("["), as.name("x"), as.name("i")), 
        jj, # x.*, i.*
        list(nomatch=nomatch, mult=mult, which=which, on=on, allow.cartesian=allow.cartesian)
    ))
    # building sql query
    subon = as.call(c(as.name("."), lapply(on, function(x) parse(text = x)[[1L]]))) # makes `.(a==a, b>=b, c<=c, d>d, e<e)` it nicely splits `"a==a"` into `==`(a, a)
    o.on = sapply(subon[-1L], function(x) as.character(x[[1L]]))
    o.on[o.on=="=="] = "=" # SQL uses `=`, not `==`
    x.on = sapply(subon[-1L], function(x) as.character(x[[2L]]))
    i.on = sapply(subon[-1L], function(x) as.character(x[[3L]]))
    sql.on = paste(sapply(seq_along(o.on), function(i) paste(paste0("x.", x.on[i]), o.on[i], paste0("i.",i.on[i]))), collapse = " AND ")
    jn.type = if (is.na(nomatch)) "LEFT OUTER" else "INNER"
    if (mult == "all") {
        sql.sel = if (which) "x.row_id AS row_id" else {
            paste(c(
                sprintf("x.%s AS \"x.%s\"", names(x), names(x)),
                sprintf("i.%s AS \"i.%s\"", names(i), names(i))
            ), collapse=", ")
        }
        sql = sprintf("SELECT %s FROM i %s JOIN x ON %s", sql.sel, jn.type, sql.on) # swap `x` with `i` to emulate right outer join, not supported in sqlite
       
    } else {
        sub.sql.sel = if (which) "x.row_id AS \"x.row_id\", i.row_id AS \"i.row_id\"" else paste(c(
            sprintf("x.%s AS \"x.%s\"", names(x), names(x)),
            sprintf("i.%s AS \"i.%s\"", names(i), names(i))
        ), collapse=", ")
        base_sql = sprintf("SELECT %s FROM i %s JOIN x ON %s", sub.sql.sel, jn.type, sql.on) # swap `x` with `i` to emulate right outer join, not supported in sqlite
        sql.sel = if (which) "x.row_id AS row_id" else {
            paste(c(
                sprintf("x.%s AS \"x.%s\"", names(x), names(x)),
                sprintf("i.%s AS \"i.%s\"", names(i), names(i))
            ), collapse=", ")
        }
        mult.fun = if (mult=="first") "MIN" else if (mult=="last") "MAX" else stop("'mult' argument accept only: all, first, last")
        mult_one = sprintf("SELECT \"i.row_id\", %s(\"x.row_id\") AS \"x.row_id\" FROM mult_all GROUP BY \"i.row_id\"", mult.fun)
        mult_join = sprintf("SELECT mult_all.%s FROM mult_all INNER JOIN mult_one ON mult_all.\"x.row_id\"==mult_one.\"x.row_id\" AND mult_all.\"i.row_id\"==mult_one.\"i.row_id\"",
                            if (which) "\"x.row_id\" AS row_id" else "*")
        sql = sprintf("WITH mult_all AS (%s), mult_one AS (%s) %s", base_sql, mult_one, mult_join)
        # WITH mult_all AS (
        #     SELECT x.id AS "x.id", x.a AS "x.a", x.b AS "x.b", x.c AS "x.c", x.row_id AS "x.row_id", i.id AS "i.id", i.a AS "i.a", i.b AS "i.b", i.c AS "i.c", i.row_id AS "i.row_id" 
        #     FROM i LEFT OUTER JOIN x 
        #     ON x.id = i.id AND x.a >= i.a
        # ), mult_one AS (
        #     SELECT "i.row_id", MIN("x.row_id") AS "x.row_id" 
        #     FROM mult_all GROUP BY "i.row_id"
        # ) SELECT mult_all.* 
        #     FROM mult_all INNER JOIN mult_one 
        #     ON mult_all."x.row_id"==mult_one."x.row_id" AND mult_all."i.row_id"==mult_one."i.row_id";
        ## debug
        # to confirm when non-equi unlocked!!
    }
    sql = paste0(sql,";")
    # run data.table and SQLite
    dtt = system.time(dtr <- eval(dtq))[[3L]]
    sqlt = system.time(sqlr <- DBI::dbGetQuery(conn, sql))[[3L]]
    if (isTRUE(verbose)) cat(sprintf("DT: %.3fs, SQL: %.3fs, ratio: %.2f, on: `%s`, nomatch: %s, mult: %s, which: %s, ans: %s rows\n", dtt, sqlt, dtt/sqlt, paste(on, collapse=" & "), toString(nomatch), toString(mult), toString(which), as.integer(nrow(sqlr))[1L]))
    # compare results
    if (which) dtr = data.table(row_id = dtr)
    r = all.equal(dtr, setDT(sqlr), ignore.row.order=TRUE)
    if (.drop) {
        DBI::dbSendQuery(conn, "DROP TABLE x;")
        DBI::dbSendQuery(conn, "DROP TABLE i;")
    }
    if (new.conn) DBI::dbDisconnect(conn)
    if (!isTRUE(r)) {
        stopifnot(is.character(r))
        c(
            sprintf("dtq: %s", paste(deparse(dtq, width.cutoff = 500L), collapse="")),
            sprintf("sql: %s", sql),
            r
        )
    } else r
}
# wrapper around set of scenarios
batch.join.sql.equal = function(x, i, on.v, nomatch.v = c(NA_integer_, 0L), mult.v = c("all"), which.v = c(TRUE, FALSE), verbose=TRUE) {
    stopifnot(is.data.table(x), is.data.table(i), is.list(on.v), is.logical(which.v), is.character(mult.v), length(on.v) >= 1L)
    # reuse single connection for faster tests - optional
    conn = DBI::dbConnect(RSQLite::SQLite())
    # reuse tables, to test if affects sqlite efficiency
    try(DBI::dbSendQuery(conn, "DROP TABLE x;"), silent = TRUE)
    try(DBI::dbSendQuery(conn, "DROP TABLE i;"), silent = TRUE)
    # row_id column required as SQL is not ordered, creating on R side
    if (!"row_id" %in% names(x)) x = data.table:::shallow(x)[, row_id := 1:.N]
    if (!"row_id" %in% names(i)) i = data.table:::shallow(i)[, row_id := 1:.N]
    DBI::dbWriteTable(conn, name = "x", value = x)
    DBI::dbWriteTable(conn, name = "i", value = i)
    len = prod(length(which.v), length(nomatch.v), length(on.v), length(mult.v))
    if (len > (len.warn <- getOption("tests.length.warning", 1e3))) warning(sprintf("You are about to run %s number of tests. To suppress this warning use 'tests.length.warning' option, set to numeric threshold or Inf.", len.warn))
    r = list()
    if (isTRUE(verbose)) {
        pt = proc.time()[[3L]]
        cat(sprintf("batch.join.sql.equal - testing nrow(x)=%s, nrow(i)=%s in total number of %s tests\n", nrow(x), nrow(i), len))
    }
    for (which in which.v) {
        which.nm = paste0("which=",toString(which))
        r[[which.nm]] = list()
        for (mult in mult.v) {
            mult.nm = paste0("mult=",toString(mult))
            r[[which.nm]][[mult.nm]] = list()
            for (nomatch in nomatch.v) {
                nomatch.nm = paste0("nomatch=",toString(nomatch))
                r[[which.nm]][[mult.nm]][[nomatch.nm]] = list()
                for (on in on.v) {
                    on.nm = paste(on, collapse="&")
                    if (!is.null(r[[which.nm]][[mult.nm]][[nomatch.nm]][[on.nm]])) {
                        warning(sprintf("Some tests are duplicated, look for %s. It will only returns TRUE if both executed calls returned TRUE.", on.nm))
                        if (!isTRUE(r[[which.nm]][[mult.nm]][[nomatch.nm]][[on.nm]])) next
                    }
                    r[[which.nm]][[mult.nm]][[nomatch.nm]][[on.nm]] = join.sql.equal(x, i, which=which, nomatch=nomatch, mult=mult, on=on, verbose=verbose, .conn=conn, .drop=FALSE)
                }
            }
        }
    }
    if (isTRUE(verbose)) cat(sprintf("batch.join.sql.equal - %s tests completed in %.1fs\n", len, proc.time()[[3L]] - pt))
    DBI::dbSendQuery(conn, "DROP TABLE x;")
    DBI::dbSendQuery(conn, "DROP TABLE i;")
    DBI::dbDisconnect(conn)
    r
}
# produces list of scenarios for a dataset
populate.on.v = function(x, i, x.on, i.on, cap=1e3, seed=123) {
    stopifnot(
        is.data.table(x), is.data.table(i), 
        length(x.on) == length(i.on),
        all.equal(x[, lapply(.SD, typeof), .SDcols=x.on], i[, lapply(.SD, typeof), .SDcols=i.on], check.attributes=FALSE)
    )
    set.seed(seed)
    r = list()
    for (i in seq_along(x.on)) {
        r[[x.on[i]]] = if (is.character(x[[x.on[i]]]) | is.factor(x[[x.on[i]]])) "==" else c(">=", "<=", ">", "<", "==")
    }
    cnt = prod(sapply(r, length))
    r = do.call(CJ, c(r, unique=TRUE))
    r = if (cnt > cap) r[sample(.N, cap)] else r[sample(.N)]
    for (i in seq_along(x.on)) { # paste x and i variables
        set(r, NULL, x.on[i], paste0(x.on[i], r[[x.on[i]]], i.on[i]))
    }
    r[, row_id := 1:.N] # to list
    lapply(split(r, by = "row_id", keep.by = FALSE, drop = TRUE), function(x) sample(unname(unlist(x))))
}
# populate data
nq_fun = function(n=100L) {
    i1 = sample(sample(n, 10L), n, TRUE)
    i2 = sample(-n/2:n/2, n, TRUE)
    i3 = sample(-1e6:1e6, n, TRUE)
    i4 = sample(c(NA_integer_, sample(-n:n, 10L, FALSE)), n, TRUE)
    
    d1 = sample(rnorm(10L), n, TRUE)
    d2 = sample(rnorm(50), n, TRUE)
    d3 = sample(c(Inf, -Inf, NA, NaN, runif(10L)), n, TRUE)
    d4 = sample(c(NA, NaN, rnorm(10L)), n, TRUE)
    
    c1 = sample(letters[1:5], n, TRUE)
    c2 = sample(LETTERS[1:15], n, TRUE)
    
    dt = data.table(i1,i2,i3,i4, d1,d2,d3,d4, c1,c2)
    if ("package:bit64" %in% search()) {
        I1 = as.integer64(sample(sample(n, 10L), n, TRUE))
        I2 = as.integer64(sample(-n/2:n/2, n, TRUE))
        I3 = as.integer64(sample(-1e6:1e6, n, TRUE))
        I4 = as.integer64(sample(c(NA_integer_, sample(-n:n, 10L, FALSE)), n, TRUE))
        dt = cbind(dt, data.table(I1,I2,I3,I4))
    }
    dt
}
