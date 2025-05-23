## Functions to let data.table play nicer with S4
if ("package:data.table" %in% search()) stopf("data.table package loaded. When developing don't load package")

## Allows data.table to be defined as an object of an S4 class,
## or even have data.table be a super class of an S4 class.
methods::setOldClass(c('data.table', 'data.frame'))

## as(some.data.frame, "data.table")
methods::setAs("data.frame", "data.table", function(from) {
  as.data.table(from)
})

## as(some.data.table, "data.frame")
methods::setAs("data.table", "data.frame", function(from) {
  as.data.frame(from)
})

methods::setOldClass(c("IDate", "Date"))
methods::setOldClass("ITime")

methods::setAs("character", "IDate", function(from) as.IDate(from))
methods::setAs("character", "ITime", function(from) as.ITime(from))
