(ns examples.employee)

(def EmployeeState [[:table ::Employee]])

(def data
  {EmployeeState [{:employee "Fred"
                   :manager "Bob"}
                  {:employee "Bob"
                   :manager "Alice"}
                  {:employee "Alice"}]})

;; (r/fixed variable) fixed point relvar
;; mat key [relvar variables]

;; :insert lookup in fixed index
;; :delete delete from fixed variable index


;; with
[[:with


  ;; variables are bound to
  [:report

   [[:from EmployeeState]
    [:where [nil? :manager]]
    [:select [:e :employee], [:reports :employee]]
    [:union [[:from EmployeeState]
             ;; fixed point join ?
             [:join :reports {:manager :employee}]
             [:select [:e :employee] [str :reports " > " :employee]]]]]]]]