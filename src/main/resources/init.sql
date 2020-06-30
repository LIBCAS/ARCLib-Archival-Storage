DELETE FROM PUBLIC.arcstorage_storage_sync_status;
DELETE FROM PUBLIC.arcstorage_storage;
DELETE FROM PUBLIC.arcstorage_aip_xml;
DELETE FROM PUBLIC.arcstorage_aip_sip;
DELETE FROM PUBLIC.arcstorage_object;
DELETE FROM PUBLIC.arcstorage_user;
DELETE FROM PUBLIC.arcstorage_system_state;

INSERT INTO PUBLIC.arcstorage_system_state(id,min_storage_count,read_only,reachability_check_interval_in_minutes) VALUES ('9a206e87-5ce9-44c2-8ab6-d35665113501',2,false,60);

INSERT INTO public.arcstorage_user(
            id, created, updated, deleted, username, password, data_space,
            user_role,email)
    VALUES ('d67f4f22-65ef-4005-94cb-ea8933ceda33', '2018-03-08 08:00:00', '2018-03-08 08:00:00', null, 'arclib-read', '$2a$10$DWIPwjkzsp/tdczMD7VtbOAtV4uTzs5ZUF7C84aIXIogn42R.F.qS', 'arclib',
            'ROLE_READ',null);
INSERT INTO public.arcstorage_user(
            id, created, updated, deleted, username, password, data_space,
            user_role,email)
    VALUES ('783d4224-3ea2-497c-9a30-678788619c47', '2018-03-08 08:00:00', '2018-03-08 08:00:00', null, 'arclib-read-write', '$2a$10$DWIPwjkzsp/tdczMD7VtbOAtV4uTzs5ZUF7C84aIXIogn42R.F.qS', 'arclib',
            'ROLE_READ_WRITE',null);
INSERT INTO public.arcstorage_user(
            id, created, updated, deleted, username, password, data_space,
            user_role,email)
    VALUES ('dd5ad2f0-4486-4429-a0e5-64f78daa918d', '2018-03-08 08:00:00', '2018-03-08 08:00:00', null, 'admin', '$2a$10$DWIPwjkzsp/tdczMD7VtbOAtV4uTzs5ZUF7C84aIXIogn42R.F.qS', null,
            'ROLE_ADMIN',null);

INSERT INTO PUBLIC.arcstorage_aip_sip (id, created, checksum_value, checksum_type, state, owner_id)
VALUES ('4b66655a-819a-474f-8203-6c432815df1f',
        '2018-03-08 08:00:00',
        '8edaa9ac5dc2d488119007cce1f4d620b24dbe68234c866b09593ddcfebae87cfce51916a59909f93abe3121f6eb00485609da8c9af85cfe74de81ddf01cc0fc' ,
        'SHA512',
        'ARCHIVED','783d4224-3ea2-497c-9a30-678788619c47');


INSERT INTO PUBLIC.arcstorage_aip_sip (id, created, checksum_value, checksum_type, state, owner_id)
VALUES ('8b2efafd-b637-4b97-a8f7-1b97dd4ee622',
        '2018-03-08 08:00:00',
        'c39f8d72e1ad0996ea2b0f363188e1626cdd12b930890d4b4da1f6079526b1a476071bc9d2132d1a6deb6c24683b695e83f135c3af971ea78631a84cfd299fac' ,
        'SHA512',
        'ARCHIVED','783d4224-3ea2-497c-9a30-678788619c47');


INSERT INTO PUBLIC.arcstorage_aip_sip (id, created, checksum_value, checksum_type, state, owner_id)
VALUES ('89f82da0-af78-4461-bf92-7382050082a1',
        '2018-03-08 08:00:00',
        '7f7d605841e307b3d730da4f17653dea50cd25b8dcad15a1c3b62bf559db582f6524b8a238d88ea7e148d6fa0f74b6dc19154ffea8bd7255324c31d1aa7ace0d' ,
        'SHA512',
        'ARCHIVED','783d4224-3ea2-497c-9a30-678788619c47');

--arclib xml 1
INSERT INTO PUBLIC.arcstorage_aip_xml (id, arcstorage_aip_sip_id, created, checksum_value, checksum_type, VERSION, state, owner_id)
VALUES ('11f82da0-af78-4461-bf92-7382050082a1',
        '4b66655a-819a-474f-8203-6c432815df1f',
        '2018-03-08 07:00:00',
        'cdfdd35d4b8b2cd44971f031d3808bc1761544d18a6990016bdfb48f169ee4b021cd0e3c764dc56848b0b1490e1a04965c5d8dada9865f8aee7098d4e0043e29' ,
        'SHA512',
        1,
        'ARCHIVED','783d4224-3ea2-497c-9a30-678788619c47');

--arclib xml 2
INSERT INTO PUBLIC.arcstorage_aip_xml (id, arcstorage_aip_sip_id, created, checksum_value, checksum_type, VERSION, state, owner_id)
VALUES ('12f82da0-af78-4461-bf92-7382050082a1',
        '4b66655a-819a-474f-8203-6c432815df1f',
        '2018-03-08 08:00:00',
        '808eacf9d69080a582a73a57d1f895ad716e07815f9fc47df49e3cc3ad6a8997dd1e9c169eb38a0d0ea20ecb4fbea95ac54784cdd8cf1095e6effd02c2a8453e' ,
        'SHA512',
        2,
        'ARCHIVED','783d4224-3ea2-497c-9a30-678788619c47');

--arclib xml 3
INSERT INTO PUBLIC.arcstorage_aip_xml (id, arcstorage_aip_sip_id, created, checksum_value, checksum_type, VERSION, state, owner_id)
VALUES ('21f82da0-af78-4461-bf92-7382050082a1',
        '8b2efafd-b637-4b97-a8f7-1b97dd4ee622',
        '2018-03-08 07:00:00',
        '4b49da182262b7212fd0440d3aa0c9b57ae6f2fc155cff2595eabcc3f3122c4b59c3b4eb2d9c2f596350bc839979174edfcc9cb4a03727d54c38d7c3048faba0' ,
        'SHA512',
        1,
        'ARCHIVED','783d4224-3ea2-497c-9a30-678788619c47');

--arclib xml 4
INSERT INTO PUBLIC.arcstorage_aip_xml (id, arcstorage_aip_sip_id, created, checksum_value, checksum_type, VERSION, state, owner_id)
VALUES ('22f82da0-af78-4461-bf92-7382050082a1',
        '8b2efafd-b637-4b97-a8f7-1b97dd4ee622',
        '2018-03-08 08:00:00',
        '0f08ecb3a7a4f4e4db2f2acae6464f03427fb26aed5ee47078b2b0b4e3172c09810a2b6c5d1de7ef9649662d01f5fbe8e4842615345d986f4ffea578e83d6087' ,
        'SHA512',
        2,
        'ARCHIVED','783d4224-3ea2-497c-9a30-678788619c47');

--arclib xml 5
INSERT INTO PUBLIC.arcstorage_aip_xml (id, arcstorage_aip_sip_id, created, checksum_value, checksum_type, VERSION, state, owner_id)
VALUES ('3182da0-af78-4461-bf92-7382050082a1',
        '89f82da0-af78-4461-bf92-7382050082a1',
        '2018-03-08 08:00:00',
        'b89ac5598fef9dafa34bb97c608ec32b067ab254575a14da646d6f67ddd18f583499526b3bbd89baf4d25ebca604e4798a80b216be25f6e23843b729ecdd9e97' ,
        'SHA512',
        1,
        'ARCHIVED','783d4224-3ea2-497c-9a30-678788619c47');


INSERT INTO PUBLIC.arcstorage_storage (id, NAME, HOST, port, priority, TYPE, note, config, reachable, synchronizing)
VALUES ('4fddaf00-43a9-485f-b81a-d3a4bcd6dd83',
        'local storage',
        'localhost',
        0,
        10,
        'FS',
        NULL,
        '{"rootDirPath":"/opt/archival-storage/data"}',
        TRUE,false);

INSERT INTO public.arcstorage_storage_sync_status(
            id, created, updated, stuck_at, phase, total_in_this_phase, done_in_this_phase,
            exception_stack_trace, exception_msg, storage_id)
    VALUES ('d685a8c0-625a-4bb0-b247-c5a8669f8d05', '2018-03-08 08:00:00', '2018-03-08 08:00:00', null, 'DONE', 1, 1, null,
            null, '4fddaf00-43a9-485f-b81a-d3a4bcd6dd83');
--INSERT INTO PUBLIC.arcstorage_storage (id, NAME, HOST, port, priority, TYPE, note, config, reachable,synchronizing)
--VALUES ('01abac74-82f7-4afc-acfc-251f912c5af1',
--        'sftp storage',
--        '192.168.10.60',
--        22,
--        1,
--        'ZFS',
--        NULL,
--        '{"rootDirPath":"/arcpool/test"}',
--        TRUE,false);
--
--
--INSERT INTO PUBLIC.arcstorage_storage (id, NAME, HOST, port, priority, TYPE, note, config, reachable,synchronizing)
--VALUES ('8c3f62c0-398c-4605-8090-15eb4712a0e3',
--        'ceph',
--        '192.168.10.61',
--        7480,
--        1,
--        'CEPH',
--        NULL,
--        '{"adapterType":"S3", "userKey":"SKGKKYQ50UU04XS4TA4O","userSecret":"TrLjA3jdlzKcvyN1vWnGqiLGDwCB90bNF71rwA5D"}',
--        TRUE,false);

