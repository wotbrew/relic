(ns com.wotbrew.tarpit.errors)

(defn fk-violation [st constraint] (ex-info "Foreign key violation" {:constraint constraint}))

(defn uk-violation [st constraint] (ex-info "Unique key violation" {:constraint constraint}))

(defn constraint-violation [st constraint]
  (case (nth constraint 0)
    :foreign-key (fk-violation st constraint)
    :unique-key (uk-violation st constraint)
    (ex-info "Constraint violation" {:constraint constraint})))

(defn extension-dep-cannot-exist [dep rel]
  (ex-info (str "Extension requires a column that does not exist: " dep) {}))

(defn restriction-dep-cannot-exist [dep rel]
  (ex-info (str "Restrict requires a column that does not exist: " dep) {}))

(defn warn-project-key-cannot-exist [k rel]
  (println "WARN projection requires a column that does not exist:" k))

(defn warn-project-away-key-cannot-exist [k rel]
  (println "WARN project-away removes a column that does not exist:" k))

(defn join-left-cannot-exist [rel1 rel2 k1 k2]
  (ex-info "Bad join" {}))

(defn join-right-cannot-exist [rel1 rel2 k1 k2]
  (ex-info "Bad join" {}))