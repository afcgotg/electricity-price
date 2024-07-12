# This is an extract from the Jupyter Notebook. There are no comments
# for now, but you can find more information by executing the notebook.
# At the end, we arrive at the electricity plan comparison per day.

using DataFrames, CSV, Plots, Dates, Statistics

raw_data = readdir("./data")
df = DataFrame()

for file in raw_data
    df_ = CSV.File("./data/" * file) |> DataFrame
    global df = vcat(df, df_)
end

rename!(df, :Fecha => :Date)
rename!(df, :Hora => :Hour)
rename!(df, :AE_kWh => :kwh)
df.kwh = parse.(Float64, replace.(df.kwh, "," => "."))

df_groupby_date = groupby(df, :Date)
summer_add_dates = filter(:Hour_maximum => col -> col == 25, combine(df_groupby_date, :Hour => maximum)).Date
df_summer_add_dates = [g for g in df_groupby_date if g.Date[1] in summer_add_dates]
for df_ in df_summer_add_dates
    df_.kwh[2] += df_.kwh[3]
    for i in 3:24
        df_.kwh[i] = df_.kwh[i+1]
    end
    delete!(df, last(indexin(eachrow(df_), eachrow(df))))
end

df_groupby_date = groupby(df, :Date)
summer_subs_dates = filter(:group_size => col -> col == 23, combine(df_groupby_date, nrow => :group_size)).Date
df_summer_subs_dates = [g for g in df_groupby_date if g.Date[1] in summer_subs_dates]
for df_ in df_summer_subs_dates
    for i in 3:23
        df_.Hour[i] += 1
    end
end

df.Hour = string.(df.Hour)
df.Hour = ":" .* df.Hour
df.DateTime = join.(eachrow(df[:,["Date", "Hour"]]))
format = dateformat"dd/mm/yyyyy:HH"
df.DateTime = DateTime.(df.DateTime, format)
df = df[:, ["Date", "DateTime", "kwh"]]
df.DayOfTheWeek = Dates.dayofweek.(df.DateTime);

df.DateTime .-= Hour(1)
df.DayOfTheWeek = Dates.dayofweek.(df.DateTime);

easy_plan = Dict(
    "P1" => 0.1328,
    "P2" => 0.1328,
    "P3" => 0.1328,
    "free_day" => 0)

daynight_plan = Dict(
    "P1" => 0.189802,
    "P2" => 0.12997,
    "P3" => 0.099999,
    "free_day" => 0)

weekend_plan = Dict(
    "P1" => 0.119999,
    "P2" => 0.16997,
    "P3" => 0.249802,
    "free_day" => 6);

function compute_price(kwh, hour, day_of_the_week, plan)
    if( day_of_the_week == get(plan, "free_day", 0))
        return 0
    elseif( 6 <= day_of_the_week <=7 ||  0 <= hour < 8 )
        return kwh * get(plan, "P3", 0)
    elseif( 8 <= hour < 10 || 14 <= hour < 18 || 22 <= hour )
        return kwh * get(plan, "P2", 0)
    elseif( 10 <= hour < 14 || 18 <= hour < 22 )
        return kwh * get(plan, "P1", 0)
    end
end

function electricity_cost(df, plan)
    cost = 0
    for row in eachrow(df)
        cost += compute_price(row.kwh, Dates.hour(row.DateTime), row.DayOfTheWeek, plan)
    end
    return cost
end

df = transform(df, [:kwh, :DateTime, :DayOfTheWeek] => ByRow((kwh, dt, dotw) -> compute_price(kwh, Dates.hour(dt), dotw, easy_plan)) => :easy_plan)
df = transform(df, [:kwh, :DateTime, :DayOfTheWeek] => ByRow((kwh, dt, dotw) -> compute_price(kwh, Dates.hour(dt), dotw, daynight_plan)) => :daynight_plan)
df = transform(df, [:kwh, :DateTime, :DayOfTheWeek] => ByRow((kwh, dt, dotw) -> compute_price(kwh, Dates.hour(dt), dotw, weekend_plan)) => :weekend_plan)

df_perday = groupby(df, :Date)
df_cost_per_day = combine(df_perday, :easy_plan => sum)
df_cost_per_day = hcat(df_cost_per_day, combine(df_perday, :daynight_plan => sum)[:,["daynight_plan_sum"]])
df_cost_per_day = hcat(df_cost_per_day, combine(df_perday, :weekend_plan => sum)[:,["weekend_plan_sum"]])

function min_col_name(row)
    values = [row.easy_plan_sum, row.daynight_plan_sum, row.weekend_plan_sum]
    col_names = [:easy_plan_sum, :daynight_plan_sum, :weekend_plan_sum]
    max_index = argmin(values)
    return col_names[max_index]
end

df_cost_per_day = transform(df_cost_per_day, AsTable([:easy_plan_sum, :daynight_plan_sum, :weekend_plan_sum]) => ByRow(min_col_name) => :min_col)

println("Print for the first 5 days of the DataFrame")
println("For more details, use \"df_cost_per_day\" DataFrame")
println("")
println(first(df_cost_per_day, 5))
