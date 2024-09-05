module covid_data_extractor

export opendisease_googlecloud, opendisease_JHUCSSE, opendisease_vaccines, oppendisease_influenza

using HTTP
using DataFrames
using JSON3
using CSV
using Dates


## Google Cloud

#function googlecloud_data_extraction(data_type; latest = true, location = nothing, file_format = :json)
#    ```
#    This function is used to extract data from Google Cloud Platform API. It receives the following parameters:
#        - `data_type`: The type of dataset to extract. It can be one of the following:
#                `aggregated`, 'index', 'demographics', 'epidemiology', 'lawatlas-emergency-declarations', 
#                'geography', 'health', 'hospitalizations', 'mobility', 'google-search-trends', 'facility-boundary-us-all',
#                'Global-vaccination-search-insights', 'vaccinations', 'oxford-government-response, 'weather', 'worldbbank',
#                or 'by-sex'.
#        - latest
#        - location
#        - file_format
#        ```
#
#    try
#        base_url = "https://storage.googleapis.com/covid19-open-data/v3/"
#        url = assemble_url(base_url, data_type; latest=latest, location=location, file_format=file_format)
#        get_data = HTTP.get(url)
#            
#        return get_data         
#    
#    catch
#        @warn "Your request is not valid. The keys 'latest' and 'location' are mutually exclusive. Also, 
#                for 'data_type = aggregated' only 'latest = true' is supported. For 'file_type' only 'json' 
#                and 'csv' formats are supported."
#    end
#end

#function googlecloud_date_filtering(data, dates; file_format = :csv)
#
#    ``` 
#        This function is used to filter the data obtained from the Google Cloud Platform API by dates. This function 
#        receives the following parameters:
#            - data: The data obtained from the Google Cloud Platform API using the 'HTTP.get()' function. The data can be 
#                obtained in 'json' or 'csv' formats using the function 'googlecloud_data_extraction'.
#            - dates: the dates to be filtered from 'data'. This should be a vector of 'Strings' or 'Date' objects following 
#                the YYYY-MM-DD format.
#            - file_format: the format of the data ('json' or 'csv').
#    ```
#
#    data = data
#    try
#        if file_format == :json
#            arr = JSON3.read(data.body)
#            date_column = findall(x -> x == "date", arr.columns)[1]
#            arr_bydate = filter(x -> x[date_column] in dates, arr.data)
#            dict = [Dict(String(k) => v for (k,v) in zip(arr_bydate.columns, vec)) for vec in arr_bydate.data]
#            df_bydate = vcat(DataFrame.(arr)...)
#        elseif file_format == :csv
#            df = CSV.read(data.body, DataFrame)
#            df_bydate = filter(x -> x.date in Dates.Date.(dates), df)
#        else
#            error("Google Cloud Platform only supports 'csv' and 'json' file formats")
#        end
#    catch
#        @warn "This type of dataset does not cointain information about dates. Please check the 
#                docstring for more information about compatibility for each 'data_type' case."
#    end
#
#    return df_bydate
#end

function assemble_url_googlecloud(base, data_type; latest = true, location = nothing, dates = nothing, file_format = :json)

    if latest
        url = base * "latest/" * string(data_type) * join(string(file_format), ".")
    elseif location != nothing
        url = base * "location/" * string(location) * "/" * string(data_type) * join(string(file_format), ".")
    else
        url = base * string(data_type) * join(string(file_format), ".")
    end

end

function opendisease_googlecloud(dates, data_type; latest = true, location = nothing, file_format = :json)

    #``` 
    #    This function is used to filter the data obtained from the Google Cloud Platform API by dates. This function 
    #    receives the following parameters:
    #        - data: The data obtained from the Google Cloud Platform API using the 'HTTP.get()' function. The data can be 
    #            obtained in 'json' or 'csv' formats using the function 'googlecloud_data_extraction'.
    #        - dates: the dates to be filtered from 'data'. This should be a vector of 'Strings' or 'Date' objects following 
    #            the YYYY-MM-DD format.
    #        - file_format: the format of the data ('json' or 'csv').
    #```
    base_url = "https://storage.googleapis.com/covid19-open-data/v3/"
    url = assemble_url_googlecloud(base_url, data_type; latest=latest, location=location, file_format=file_format)
    data = HTTP.get(url)
    
    try
        if file_format == :json
            arr = JSON3.read(data.body)
            date_column = findall(x -> x == "date", arr.columns)[1]
            arr_bydate = filter(x -> x[date_column] in dates, arr.data)
            dict = [Dict(String(k) => v for (k,v) in zip(arr_bydate.columns, vec)) for vec in arr_bydate.data]
            df_bydate = vcat(DataFrame.(arr)...)
        elseif file_format == :csv
            df = CSV.read(data.body, DataFrame)
            df_bydate = filter(x -> x.date in Dates.Date.(dates), df)
        else
            error("Google Cloud Platform only supports 'csv' and 'json' file formats")
        end
    catch
        @warn "This type of dataset does not cointain information about dates. Please check the 
                docstring for more information about compatibility for each 'data_type' case."
    end

    return df_bydate
end


## JHUCSSE

function assemble_base_url(base; data_type) 
    data_type == :totals ? (url = base * "jhucsse/") : (url = base * "historical/")
    return url
end

function assemble_url_jhucsse(base_url, data_type::Val{:totals}; countries = nothing, provinces = nothing, USstates = nothing, UScounties = nothing) 
    if countries == :all 
        url = base_url
    elseif UScounties != nothing
        url = base_url * "counties"  
    else
        error("This method only applies for assembling url for total COVID-19 data. Please remember that
        'counties' and 'countries' are mutually exclusive")
    end
end


function assemble_url_jhucsse(base_url, data_type::Val{:accumulated}; countries = nothing, provinces = nothing, USstates = nothing, UScounties = nothing) 
    countries == :all ? (url = base_url * "all") : error("For accumulated data type, the url can only be assembled for all the countries")
end


function assemble_url_jhucsse(base_url, data_type::Val{:timeseries}; countries = nothing, provinces = nothing, USstates = nothing, UScounties = nothing) 
   
    if (countries != nothing && provinces == nothing)
        countries == :all ? (url = base_url) : (url = base_url * join(countries, ","))
    
    elseif provinces !=  nothing
        if length(provinces) == 1                
            url = base_url * string.(countries[1]) * "/" * string.(provinces[1])
        elseif length(provinces) > 1
            url = base_url * string.(countries[1]) * "/" * join(map(x -> replace(x, " " => "%20"), string.(provinces)), ",")
        else
            error("'provinces' must be a vector of 'string's of 'length > 0' ")
        end
    
    elseif UScounties != nothing
        counties_url = base_url * "usacounties"
        if UScounties == :all
            url = counties_url
        elseif USstates != nothing
            url = counties_url * "/" * string(USstates[1]) * "/"
        else
            error("An url can only be assembled for one county or for all the countries from one state")
        end           
        
    else
        error("A single country must be specified to generate 'provinces' time series url.")
    end
end


function assemble_url_jhucsse(base_url, data_type::Symbol; kwargs...) 
    try
       assemble_url_jhucsse(base_url, Val(data_type); kwargs...)
    catch e
        error("It was not possible to assemble the url as indicated by the arguments.")
    end
    
end

function opendisease_JHUCSSE(data_type::Val{:totals}; lastdays = nothing, countries = nothing, provinces = nothing, USstates = nothing, UScounties = nothing)   
    """
    'countries': An array of country names in iso2, iso3, or country ID code format.
    'provinces': An array of 'Symbol'. It contains provinces names spelled correctly separated by ','.
    """
    # Total cases are only available for counties and for all the contries
    try
        base_jhucsse = "https://disease.sh/v3/covid-19/"
        base_url = assemble_base_url(base_jhucsse, data_type = data_type)
        params = Dict("lastdays" => lastdays)
            
        if countries != nothing
            countries == :all ? (url = base_url) : (url = assemble_url_jhucsse(base_url, data_type = data_type, countries = countries)) 
            get_data = HTTP.get(url)
            raw_df = vcat(DataFrame.(JSON3.read(get_data.body))...)
            stats = vcat(DataFrame.(raw_df[!, :stats])...)
            coordinates = vcat(DataFrame.(raw_df[!, :coordinates])...)
            data = hcat(select(raw_df, [:country, :county, :province, :updatedAt]), stats, coordinates)
            
        elseif UScounties != nothing
            url = assemble_url_jhucsse(base_url, data_type, UScounties = UScounties)
            get_data =  HTTP.get(url)   #get_data = HTTP.get("https://disease.sh/v3/covid-19/jhucsse/counties")
            raw_df = vcat(DataFrame.(JSON3.read(get_data.body))...)
            stats = vcat(DataFrame.(raw_df[!, :stats])...)
            coordinates = vcat(DataFrame.(raw_df[!, :coordinates])...)
            df = hcat(select(raw_df, [:country, :county, :province, :updatedAt]), stats, coordinates)
            if UScounties == :all 
                data = df 
            elseif USstates != nothing
                data = filter(x -> x.province in string.(USstates), filter(x -> x.county in string.(UScounties), df))
            else
                data = filter(x -> x.county in string.(UScounties), df)  #UScounty is a vector of Symbols
            end
            
        else
            error("COVID-19 Totals are only available for all the countries ('countries = :all') and for US counties 
                (all counties 'counties = :all', or a subset of them 'counties = (:countie1, :countie2, ...))'.")
        end
    
        return data
   
    catch e
        print(e)        
    end
end

function opendisease_JHUCSSE(data_type::Val{:accumulated}; lastdays = nothing, countries = nothing, provinces = nothing, USstates = nothing, UScounties = nothing)   
    """
    'countries': An array of country names in iso2, iso3, or country ID code format.
    'provinces': An array of 'Symbol'. It contains provinces names spelled correctly separated by ','.
    """
    
    try
        base_jhucsse = "https://disease.sh/v3/covid-19/"
        base_url = assemble_base_url(base_jhucsse, data_type = data_type)
        params = Dict("lastdays" => lastdays)
        countries == :all ? (url = assemble_url_jhucsse(base_url, data_type, countries = countries)) : error("Accumulated data is only available for all the countries")
        get_data = HTTP.get(url, query = params)
        data = DataFrame(JSON3.read(get_data.body))
        #data = assemble_df_jhucsse(df)
            
        return data
   
    catch e
        print(e)        
    end
end

function opendisease_JHUCSSE(data_type::Val{:timeseries}; lastdays = nothing, countries = nothing, provinces = nothing, USstates = nothing, UScounties = nothing)
    """
    'countries': An array of country names in iso2, iso3, or country ID code format.
    'provinces': An array of 'Symbol'. It contains provinces names spelled correctly separated by ','.
    """
    
    try
        base_jhucsse = "https://disease.sh/v3/covid-19/"
        base_url = assemble_base_url(base_jhucsse, data_type = data_type)
        params = Dict("lastdays" => lastdays)
       
        if (countries != nothing && provinces == nothing)
            if countries == :all
                url = assemble_url_jhucsse(base_url, data_type, countries = countries)
                get_data = HTTP.get(url, query = params)
                df  = DataFrame(JSON3.read(get_data.body))
                data = assemble_df_jhucsse(df, countries = countries)
                #timeline = vcat(DataFrame.(df[:, :timeline])...)
                #data = hcat(select(df, Not(:timeline)), timeline)
                
            elseif countries != :all
                url = assemble_url_jhucsse(base_url, data_type, countries = countries)
                get_data = HTTP.get(url, query = params)
                length(countries) == 1 ? (df = DataFrame(JSON3.read(get_data.body))) : (df = vcat(DataFrame.(JSON3.read(get_data.body))...))
                data = assemble_df_jhucsse(df, countries = countries)
                #timeline = vcat(DataFrame.(df[:, :timeline])...)
                #data = hcat(select(df, Not(:timeline)), timeline)
            else
                error("'countries' must be ':all' or a vector of 'Symbol's with the name of the countries of interest")
            end   
        
        elseif provinces != nothing
            #provinces names must not be capitalized and the words must be separated by one space. Example: "australian capital territory"
            url = assemble_url_jhucsse(base_url, data_type, countries = countries, provinces = provinces)
            get_data = HTTP.get(url, query = params)
            df = DataFrame(JSON3.read(get_data.body))
            data = assemble_df_jhucsse(df, countries = countries)
            #timeline = vcat(DataFrame.(df[:, :timeline])...)
            #data = hcat(select(df, Not(:timeline)), timeline)
            #data = filter(x -> x.province in pronvinces, df
        elseif UScounties == :all
            url = assemble_url_jhucsse(base_url, data_type, UScounties = UScounties)
            get_data = HTTP.get(url, query = params)
            data = DataFrame(USstate = JSON3.read(get_data.body))
            #USstates == nothing ? data = filter(x -> x.county in string.(UScounties), df) : data = df
        
        elseif USstates != nothing
            url = assemble_url_jhucsse(base_url, data_type, USstates = USstates, UScounties = UScounties)
            get_data = HTTP.get(url, query = params)
            raw_df = DataFrame(JSON3.read(get_data.body))
            df = assemble_df_jhucsse(raw_df, USstates = USstates)
            #timeline = vcat(DataFrame.(raw_df[:, :timeline])...)
            #df = hcat(select(raw_df, Not(:timeline)), timeline)
            UScounties != nothing ? data = filter(x -> x.county in string.(UScounties), df) : data = df
        else
            nothing
        end
                                     
        return data
   
    catch e
        print(e) 
    end
end

function opendisease_JHUCSSE(data_type::Symbol; kwargs...)
    """
    'countries': An array of country names in iso2, iso3, or country ID code format.
    'provinces': An array of 'Symbol'. It contains provinces names spelled correctly separated by ','.
    """
    try
        opendisease_JHUCSSE(Val(data_type); kwargs...)
    catch e
        error("It was not possible to request the data as indicated. Please check if following requirements 
                are met in your data request: \n
                - COVID-19 Totals are only available for all the countries ('countries = :all') and for US counties 
                (all counties 'counties = :all', or a subset of them 'counties = (:countie1, :countie2, ...))'. \n
                - 'lastdays' only applies for time-series data, and it must be always specified. \n 
                - 'countries' must be a vesctor of countries' IDs, iso2 or iso3 \n")
         # for Time series data, the number of days (lastdays) must be specified \n
         # lastdays only applies for time series \n
    end 
end


## Vaccines

function assemble_url_vaccines(base_url, data_type; countries = nothing, USstates = nothing)
    base = base_url * "vaccine/"
    if data_type == :trials
        url = base
    elseif data_type == :doses
        doses_url = base * "coverage/"
        
        if countries != nothing
            countries_url = doses_url * "countries/"
            countries == :all ? (url = countries_url) : (url = countries_url * string.(countries))
        
        elseif USstates!= nothing
            USstates_url = doses_url * "states/"
            USstates == :all ? (url = USstates_url) : (url = USstates_url * string.(USstates))
        else
            error("")
        end
        
    else
        error("")
    end
    
    return url
    
end

function assemble_df(get_data; all_data = true, order_by = :country, vaccine_timeline = :full)
    #order_by can be :state, :country
     
     if all_data
         
         if vaccine_timeline == :simple
             dict = [Dict(order_by => j[string(order_by)], :date => date, :total_cases => cases) 
                 for j in JSON3.read(get_data.body) for (date, cases) in j["timeline"]]
             data = DataFrame(dict)
         elseif vaccine_timeline == :full
             raw_df = vcat(DataFrame.(JSON3.read(get_data.body))...)
             timeline = vcat(DataFrame.(raw_df.timeline)...)
             data = hcat(select(raw_df, Not(:timeline)), timeline)    
         
         else 
             error("")
         end
         
     
     elseif all_data == false
                 
         if vaccine_timeline == :simple
             raw_data = JSON3.read(get_data.body)
             dict = [Dict(:state => raw_data[string(order_by)], :date => date, :total_cases => cases) 
                      for (date, cases) in raw_data["timeline"]]
             data = DataFrame(dict)     
             
         elseif vaccine_timeline == :full
             raw_df = DataFrame(JSON3.read(get_data.body))
             data = hcat(select(raw_df, Not(:timeline)), DataFrame(raw_df.timeline))
         else
             error()
         end
                 
     end
         
     return data
 end

function opendisease_vaccines(data_type::Val{:trials}; countries = nothing, USstates = nothing, lastdays = nothing, vaccine_timeline = :simple)
    try    
        #vaccine_timeline can be :simple of :full
        base_url = "https://disease.sh/v3/covid-19/"
        url = assemble_url(base_url, data_type)
        get_data = HTTP.get(url)
        data = collect(JSON3.read(get_data.body))
        
        return data  
    catch e
        print(e)
    end 
end

function opendisease_vaccines(data_type::Val{:doses}; countries = nothing, USstates = nothing, lastdays = nothing, vaccine_timeline = :simple)
    try    
        #vaccine_timeline can be :simple of :full
        base_url = "https://disease.sh/v3/covid-19/"            
        vaccine_timeline == :simple ? (fullData = false) : (fullData = true)
        params = Dict("lastdays" =>  lastdays, "fullData" => fullData)
        countries != nothing ? (order_by = :country) : (order_by = :state)
        (countries == :all || USstates == :all) ? (all_data = true) : (all_data = false)
        
        if countries != nothing
            if countries == :all
                url = assemble_url(base_url, data_type, countries = countries)
                get_data = HTTP.get(url, query = params)
                data = assemble_df(get_data, all_data = all_data, order_by = order_by, vaccine_timeline = vaccine_timeline)
                
            else 
                arr_df = []
                for country in 1:length(countries)
                    url = assemble_url(base_url, data_type, countries = countries[country])
                    get_data = HTTP.get(url, query = params)
                    df = assemble_df(get_data, all_data = all_data, order_by = order_by, vaccine_timeline = vaccine_timeline)
                    push!(arr_df, df)
                end
                data = vcat(arr_df...)   
            end
                                 
        elseif USstates != nothing 
            if USstates == :all
                url = assemble_url(base_url, data_type, USstates = USstates)
                get_data = HTTP.get(url, query = params)
                data = assemble_df(get_data, all_data = all_data, order_by = order_by, vaccine_timeline = vaccine_timeline)  
                        
            else
                arr_df = []
                for state in 1:length(USstates)
                    url = assemble_url(base_url, data_type, USstates = USstates[state])
                    get_data = HTTP.get(url, query = params)
                    df = assemble_df(get_data, all_data = all_data, order_by = order_by, vaccine_timeline = vaccine_timeline)
                    push!(arr_df, df)                        
                end
                data = vcat(arr_df...)
            end
            
        else
            error("'vaccine_timeline' must be one of :simple or :full")
        end
        
        return data
        
    catch e
        print(e)
    end
end


function opendisease_vaccines(data_type::Symbol; kwargs...)
    try
        opendisease_vaccines(Val(data_type); kwargs...)
    catch e
       print(e) 
    end 
end


## Influenza

function assemble_url_influenza(base_url, data_type; reported_by = nothing)
    base = base_url * "influenza/cdc/"
    if data_type == :influenza_like
        url = base * "ILINet"
    elseif data_type == :reported
        if reported_by == :clinical_labs
            url = base * "USCL"
        elseif reported_by == :public_health_labs
            url = base * "USPHL"
        else
            error("'reported_by' must be one of :clinical_labs or :public_health_labs")
        end
    else
        error("Influenza-like illnesses are reported by the US Center for Disease Control, and therefore,
        'data_type = :influenza_like' is only compatible with 'reported_by = nothing'. For reported data 
        ('data_type = :reported'), there are two sources: clinical labs ('reported_by = :clinical_labs')
        and public health labs ('reported_by = :public_health_labs').")
    end
end

function opendisease_influenza(data_type; reported_by = nothing)
    try
        base_url = "https://disease.sh/v3/"
        url = assemble_url_influenza(base_url, data_type; reported_by = reported_by)
        get_data = HTTP.get(url)
        data = DataFrmae(JSON3.read(get_data.body).data)
        
    catch e
        print(e)
        
    finally
    end

    return data

end

end #module