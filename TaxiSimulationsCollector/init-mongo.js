db.createUser(
    {
        user: "admin",
        pwd: "dcdb47b163235e575652c63f1825fd86",
        roles: [
            {
                role: "readWrite",
                db: "simulations"
            }
        ]
    }
    )