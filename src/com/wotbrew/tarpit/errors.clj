(ns com.wotbrew.tarpit.errors)

(defn fk-violation [st constraint] (ex-info "Foreign key violation" {:constraint constraint}))

(defn uk-violation [st constraint] (ex-info "Unique key violation" {:constraint constraint}))

(defn constraint-violation [st constraint]
  (case (nth constraint 0)
    :foreign-key (fk-violation st constraint)
    :unique-key (uk-violation st constraint)
    (ex-info "Constraint violation" {:constraint constraint})))