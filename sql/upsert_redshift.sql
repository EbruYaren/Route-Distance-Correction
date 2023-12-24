create temp table IF NOT EXISTS temp_{target} (like {target});
        truncate temp_{target};
        copy temp_{target} from 's3://{s3_bucket_name}/{file_name}'
        credentials 'aws_access_key_id={s3_access_key};aws_secret_access_key={s3_secret_key}'
        region '{s3_region}' delimiter ',' ignoreheader 1;
        begin transaction;
        delete from {target}
        using temp_{target}
        where {target}.{unique_id} = temp_{target}.{unique_id};
        insert into {target}
        select * from temp_{target};
        end transaction;
        drop table temp_{target};