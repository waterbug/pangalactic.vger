# Interactive testing for pangalactic.vger (repository server)

[0] get or create a private key and certificate for localhost and copy them
    into the `.crossbar_for_test_vger` directory and name them server_key.pem
    and server_cert.pem, respectively (crossbar's config file,
    .crossbar_for_test_vger/config.json, specifies those names).

[1] start crossbar message server:

    ./crossbar_for_test_vger.sh

[2] start vger:

    python ~/pangalactic.vger/pangalactic/vger/vger.py \
        --home ~/vger_home \
        --db_url postgresql://user@localhost:5432/vgerdb \
        --debug \
        --test

